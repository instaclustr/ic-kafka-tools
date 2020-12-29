
/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.admin

import java.util
import java.util.stream.Collectors
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit

import joptsimple._
import org.apache.kafka.common.TopicPartitionInfo

import scala.collection.mutable
import kafka.common.AdminCommandFailedException
import kafka.utils.Whitelist
import kafka.log.LogConfig
import kafka.server.ConfigType
import kafka.utils.Implicits._
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{
  AdminClientConfig,
  KafkaAdminClient,
  ListTopicsOptions,
  TopicListing,
  CreateTopicsOptions,
  NewTopic,
  ConfigEntry,
  Config,
  NewPartitions,
  DeleteTopicsOptions, DescribeTopicsResult,
  AdminClient => JAdminClient,
  DescribeClusterOptions,
  AlterConfigsOptions}
import org.apache.kafka.common.config.{ConfigResource}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.collection.JavaConverters._
import scala.collection._

object IcTopicCommand extends Logging {

  var futuresTimeoutMs: Int = 20000

  def main(args: Array[String]): Unit = {

    val opts = new IcTopicCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "ic-kafka-topics: Create, delete, describe, or change a topic.")

    // should have exactly one action
    val actions = Seq(opts.createOpt, opts.listOpt, opts.alterOpt, opts.describeOpt, opts.deleteOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --create, --alter or --delete")

    opts.checkArgs()

    val time = Time.SYSTEM

    if (opts.options.has(opts.customTimeout))
      futuresTimeoutMs = opts.options.valueOf(opts.customTimeout).toInt

    val props = new Properties()
    // if there is a properties file load the properties now
    if (opts.options.has(opts.propertiesFile)) {
      val propertiesFilePath = opts.options.valueOf(opts.propertiesFile)
      props.load(scala.io.Source.fromFile(propertiesFilePath).reader())
    }

    val kafkaConnection = opts.options.valueOf(opts.kafkaConnectOpt)
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnection)
    val adminClient = JAdminClient.create(props)

    var exitCode = 0
    try {
      if(opts.options.has(opts.createOpt))
        createTopic(adminClient, opts)
      else if(opts.options.has(opts.alterOpt))
        alterTopic(adminClient,  opts)
      else if(opts.options.has(opts.listOpt))
        listTopics(adminClient, opts)
      else if(opts.options.has(opts.describeOpt))
        describeTopic(adminClient,  opts)
      else if(opts.options.has(opts.deleteOpt))
        deleteTopic(adminClient, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      if (adminClient != null)
        adminClient.close (5000,TimeUnit.MILLISECONDS )
      Exit.exit(exitCode)
    }
  }

  private def getTopics(adminClient: JAdminClient, opts: IcTopicCommandOptions) = {
    val allTopics = adminClient.listTopics(new ListTopicsOptions().listInternal(true)).listings().get(futuresTimeoutMs, TimeUnit.MILLISECONDS ).asScala
        .map(d => d.name())
        .toSeq
        .sorted
    if (opts.options.has(opts.topicOpt)) {
      val topicsSpec = opts.options.valueOf(opts.topicOpt)
      val topicsFilter = new Whitelist(topicsSpec)
      allTopics.filter(topicsFilter.isTopicAllowed(_, excludeInternalTopics = false))
    } else
      allTopics
  }

  def createTopic(adminClient: JAdminClient, opts: IcTopicCommandOptions) {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    val ifNotExists = opts.options.has(opts.ifNotExistsOpt)
    if (Topic.hasCollisionChars(topic))
      println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.")

    var newTopic:NewTopic = null

    if (opts.options.has(opts.replicaAssignmentOpt)) {
      val assignment: util.Map[Integer, util.List[Integer]] = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
      newTopic = new NewTopic(topic, assignment)
    } else {
      CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
      val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
      val replicas = opts.options.valueOf(opts.replicationFactorOpt).shortValue
      newTopic = new NewTopic(topic,partitions,replicas)
    }

    if(opts.options.has(opts.configOpt)) {
      val configsToBeAdded = parseTopicConfigsToBeAdded(opts).asScala.asJava
      newTopic.configs(configsToBeAdded)
    }

    val createTopicOptions = new CreateTopicsOptions()

    try {
      adminClient.createTopics(Seq(newTopic).asJavaCollection, createTopicOptions).all().get(futuresTimeoutMs, TimeUnit.MILLISECONDS)
      println("Created topic \"%s\".".format(topic))
    } catch {
      case e: java.util.concurrent.ExecutionException  => {
        val causeOption: Option[Throwable] = Option(e.getCause)
        causeOption match {
          case Some(cause) =>
            if (!(ifNotExists && cause.isInstanceOf[TopicExistsException])) throw e
          case None =>
            throw e
        }
      }
      case defaultException => throw defaultException
    }
  }



  def alterTopic(adminClient: JAdminClient, opts: IcTopicCommandOptions) {
    val topics = getTopics(adminClient, opts)
    println (topics.mkString("\n"))
    val ifExists = opts.options.has(opts.ifExistsOpt)
    if (topics.isEmpty && !ifExists) {
      throw new IllegalArgumentException("Topic %s does not exist on  %s".format(opts.options.valueOf(opts.topicOpt),
        opts.options.valueOf(opts.kafkaConnectOpt)))
    }

    // process list as multiple requests to AdminClient (new admin client actually supports multiple requests in a batch, but handling like this
    // to be more in-pattern with kafka-topics
    topics.foreach { topic =>

      val configResource = new ConfigResource(ConfigResource.Type.TOPIC,topic)

      // get the current config for the topic
      val describeConfigsResults = adminClient.describeConfigs(Collections.singletonList(configResource))
        .all()
        .get(futuresTimeoutMs,TimeUnit.MILLISECONDS)
        .asScala

      // get the first result (we are only expecting one because we only provided one topic name)
      val config : Config = describeConfigsResults.head._2

      // filter down to just the config values that are not default configs
      val nonDefaultConfigs = config.entries().asScala.iterator
                                  .filter (d => !d.isDefault() )

      // create a Properties object and populate it with config results
      val configs = new Properties()
      nonDefaultConfigs
        .foreach(  (configEntry : ConfigEntry) => configs.put (configEntry.name,configEntry.value))

      if(opts.options.has(opts.configOpt) || opts.options.has(opts.deleteConfigOpt)) {
        // println("WARNING: Altering topic configuration from this script has been deprecated and may be removed in future releases.")
        // println("         Going forward, please use kafka-configs.sh for this functionality")

        val configsToBeAdded = parseTopicConfigsToBeAdded(opts)
        val configsToBeDeleted = parseTopicConfigsToBeDeleted(opts)
        // compile the final set of configs to be applied
        configs ++= configsToBeAdded
        configsToBeDeleted.foreach(config => configs.remove(config))

        val topicConfigEntries = configs.asScala.iterator.map( d =>  new ConfigEntry (d._1,d._2)).toSeq.asJava


        val v = adminClient.alterConfigs(Map( configResource -> new Config(topicConfigEntries) ).asJava, new AlterConfigsOptions().validateOnly(false))
          .all()
          .get(futuresTimeoutMs,TimeUnit.MILLISECONDS)

        println("Updated config for topic \"%s\".".format(topic))
      }

      if(opts.options.has(opts.partitionsOpt)) {
        if (topic == Topic.GROUP_METADATA_TOPIC_NAME) throw new IllegalArgumentException("The number of partitions for the offsets topic cannot be changed.")
        println("WARNING: If partitions are increased for a topic that has a key, the partition " +
          "logic or ordering of the messages will be affected")
        val nPartitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val topicDescription = adminClient.describeTopics(Set(topic).asJavaCollection)
          .all()
          .get(futuresTimeoutMs,TimeUnit.MILLISECONDS)
          .asScala
          .head
          ._2 // now we've got a single TopicDescription
          //.entrySet()
          //.stream()
          //.findFirst()
          //.orElse(null)  // todo trap the NPE that will occur if the topic isn't found...
          //.getValue() // whew, now we've got a single TopicDescription

        val existingAssignment : mutable.Map[Int, Seq[Int]] = mutable.Map()
        topicDescription
          .partitions()
          .asScala
          .foreach { partition  => existingAssignment.put (partition.partition(), partition.replicas.asScala.toSeq.map( n => n.id))}

        if (existingAssignment.isEmpty)
          throw new InvalidTopicException(s"The topic $topic does not exist")
        if (opts.options.has(opts.replicaAssignmentOpt)) {
          val replicaAssignmentStr = opts.options.valueOf(opts.replicaAssignmentOpt)
          val newAssignment = replicaAssignmentStr
                                .split(",")
                                .toList.map(_.split(":")
                                .map(d => new Integer(d.toInt)).toList.asJava)
                                .asJava
          val newPartitions = NewPartitions.increaseTo(nPartitions, newAssignment) // specifies the new partition size and the broker assignments for each replica of the partition
          adminClient.createPartitions( Map(topic -> newPartitions).asJava )
        }
        else
        {
          val newPartitions = NewPartitions.increaseTo(nPartitions) // specifies the new partition size (with default broker assignments for each partition)
          adminClient.createPartitions( Map(topic -> newPartitions).asJava )
        }

        println("Adding partitions succeeded")
      }
    }
  }

  def listTopics(adminClient: JAdminClient, opts: IcTopicCommandOptions) {
    val topics = getTopics(adminClient, opts)
    println(topics.mkString("\n"))
  }

  def deleteTopic(adminClient: JAdminClient, opts: IcTopicCommandOptions) {
    val topics = getTopics(adminClient, opts)
    val ifExists = opts.options.has(opts.ifExistsOpt)
    if (topics.isEmpty && !ifExists) {
      throw new IllegalArgumentException("Topic %s does not exist on %s".format(opts.options.valueOf(opts.topicOpt),
        opts.options.valueOf(opts.kafkaConnectOpt)))
    }
    topics.foreach { topic =>
      try {
        if (Topic.isInternal(topic)) {
          throw new AdminOperationException("Topic %s is a kafka internal topic and is not allowed to be marked for deletion.".format(topic))
        } else {
          val deleteTopicsResult = adminClient.deleteTopics(Seq(topic).asJavaCollection).all().get(futuresTimeoutMs,TimeUnit.MILLISECONDS)
          println("Topic %s is marked for deletion.".format(topic))
          println("Note: This will have no impact if delete.topic.enable is not set to true.")
        }
      } catch {
        case _: Throwable =>
          throw new AdminOperationException("Error while deleting topic %s".format(topic))
      }
    }
  }

  def describeTopic(adminClient: JAdminClient, opts: IcTopicCommandOptions) {
    val topics = getTopics(adminClient, opts)
    val reportUnderReplicatedPartitions = opts.options.has(opts.reportUnderReplicatedPartitionsOpt)
    val reportUnavailablePartitions = opts.options.has(opts.reportUnavailablePartitionsOpt)
    val reportOverriddenConfigs = opts.options.has(opts.topicsWithOverridesOpt)

    adminClient.describeTopics(topics.asJavaCollection).all().get(futuresTimeoutMs,TimeUnit.MILLISECONDS).asScala.foreach { case(topic,topicDescription) =>
      val describeConfigs: Boolean = !reportUnavailablePartitions && !reportUnderReplicatedPartitions
      val describePartitions: Boolean = !reportOverriddenConfigs
      val sortedPartitions = topicDescription.partitions().asScala.sortBy (_.partition())

      if (describeConfigs) {
        val config = adminClient.describeConfigs(Seq(new ConfigResource(ConfigResource.Type.TOPIC, topic)).asJava)
          .all().get(futuresTimeoutMs,TimeUnit.MILLISECONDS).asScala.head._2

        val configs = config.entries().asScala.iterator
                        .filter(d => !d.isDefault)
                        .map(d => d.name() + "=" + d.value())
        if (!reportOverriddenConfigs || config.entries().size() != 0) {
          val numPartitions = topicDescription.partitions.size
          val replicationFactor = topicDescription.partitions().asScala.head.replicas.size
          val configsAsString = configs.mkString(",")
          println("Topic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:%s"
            .format(topic, numPartitions, replicationFactor, configsAsString))
        }
      }

      if (describePartitions) {
        sortedPartitions.foreach { topicPartitionInfo =>
          val inSyncReplicas = topicPartitionInfo.isr().asScala
          val leader = topicPartitionInfo.leader().id()
          if ((!reportUnderReplicatedPartitions && !reportUnavailablePartitions) ||
            (reportUnderReplicatedPartitions && inSyncReplicas.size < topicPartitionInfo.replicas().size) ||
            (reportUnavailablePartitions && (topicPartitionInfo.leader().isEmpty /*|| !liveBrokers.contains(leader.get) */))) {

            print("\tTopic: " + topic)
            print("\tPartition: " + topicPartitionInfo.partition())
            print("\tLeader: " + (if(topicPartitionInfo.leader != null && !topicPartitionInfo.leader().isEmpty) topicPartitionInfo.leader().id else "none"))
            print("\tReplicas: " + topicPartitionInfo.replicas.asScala.toStream.map(_.id).mkString(","))
            print("\tIsr: " + topicPartitionInfo.isr().asScala.toStream.map (_.id).mkString(","))
            println()
          }
        }
      }
    }
  }


  def parseTopicConfigsToBeAdded(opts: IcTopicCommandOptions): Properties = {
    val configsToBeAdded = opts.options.valuesOf(opts.configOpt).asScala.map(_.split("""\s*=\s*"""))
    require(configsToBeAdded.forall(config => config.length == 2),
      "Invalid topic config: all configs to be added must be in the format \"key=val\".")
    val props = new Properties
    configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).trim))
    LogConfig.validate(props)
    if (props.containsKey(LogConfig.MessageFormatVersionProp)) {
      println(s"WARNING: The configuration ${LogConfig.MessageFormatVersionProp}=${props.getProperty(LogConfig.MessageFormatVersionProp)} is specified. " +
        s"This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker.")
    }
    props
  }

  def parseTopicConfigsToBeDeleted(opts: IcTopicCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfigOpt)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfigOpt).asScala.map(_.trim())
      val propsToBeDeleted = new Properties
      configsToBeDeleted.foreach(propsToBeDeleted.setProperty(_, ""))
      LogConfig.validateNames(propsToBeDeleted)
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  def parseReplicaAssignment(replicaAssignmentList: String): util.Map[Integer, util.List[Integer]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Integer, util.List[Integer]]()
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => new Integer(s.trim().toInt))
      val duplicateBrokers = CoreUtils.duplicates(brokerList)
      if (duplicateBrokers.nonEmpty)
        throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicateBrokers.mkString(",")))
      ret.put(new Integer(i), brokerList.toList.asJava)
      if (ret(i).size != ret(0).size)
        throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList)
    }
    ret.toMap.asJava
  }

  class IcTopicCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val kafkaConnectOpt = parser.accepts("bootstrap-server", "REQUIRED: The connection string for the Kafka connection in the form host:port. ")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])
    val listOpt = parser.accepts("list", "List all available topics.")
    val createOpt = parser.accepts("create", "Create a new topic.")
    val deleteOpt = parser.accepts("delete", "Delete a topic")
    val alterOpt = parser.accepts("alter", "Alter the number of partitions, replica assignment, and/or configuration for the topic.")
    val describeOpt = parser.accepts("describe", "List details for the given topics.")
    val helpOpt = parser.accepts("help", "Print usage information.")
    val topicOpt = parser.accepts("topic", "The topic to be create, alter or describe. Can also accept a regular " +
      "expression except for --create option")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val nl = System.getProperty("line.separator")
    val configOpt = parser.accepts("config", "A topic configuration override for the topic being created or altered."  +
      "The following is a list of valid configurations: " + nl + LogConfig.configNames.map("\t" + _).mkString(nl) + nl +
      "See the Kafka documentation for full details on the topic configs.")
      .withRequiredArg
      .describedAs("name=value")
      .ofType(classOf[String])
    val deleteConfigOpt = parser.accepts("delete-config", "A topic configuration override to be removed for an existing topic (see the list of configurations under the --config option).")
      .withRequiredArg
      .describedAs("name")
      .ofType(classOf[String])
    val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
      "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected")
      .withRequiredArg
      .describedAs("# of partitions")
      .ofType(classOf[java.lang.Integer])
    val replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created.")
      .withRequiredArg
      .describedAs("replication factor")
      .ofType(classOf[java.lang.Short])
    val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created or altered.")
      .withRequiredArg
      .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
        "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
      .ofType(classOf[String])
    val propertiesFile = parser.accepts( "properties-file", "A file containing connection properties")
      .withRequiredArg()
      .describedAs ("a path and filename")
      .ofType(classOf[String])
    val customTimeout = parser.accepts("timeout", "Configurable query timeout")
      .withRequiredArg()
      .describedAs( "milliseconds")
      .defaultsTo("10000")
      .ofType(classOf[java.lang.Integer])
    val reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
      "if set when describing topics, only show under replicated partitions")
    val reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
      "if set when describing topics, only show partitions whose leader is not available")
    val topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
      "if set when describing topics, only show topics that have overridden configs")
    val ifExistsOpt = parser.accepts("if-exists",
      "if set when altering or deleting topics, the action will only execute if the topic exists")
    val ifNotExistsOpt = parser.accepts("if-not-exists",
      "if set when creating topics, the action will only execute if the topic does not already exist")

    //val disableRackAware = parser.accepts("disable-rack-aware", "Disable rack aware replica assignment")

    val forceOpt = parser.accepts("force", "Suppress console prompts")

    val options = parser.parse(args : _*)

    val allTopicLevelOpts: Set[OptionSpec[_]] = Set(alterOpt, createOpt, describeOpt, listOpt, deleteOpt)

    def checkArgs() {
      // check required args
      // CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      CommandLineUtils.checkRequiredArgs(parser, options, kafkaConnectOpt)

      if (!options.has(listOpt) && !options.has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, configOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, deleteConfigOpt, allTopicLevelOpts -- Set(alterOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, partitionsOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicationFactorOpt, allTopicLevelOpts -- Set(createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, allTopicLevelOpts -- Set(createOpt,alterOpt))
      if(options.has(createOpt))
        CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, Set(partitionsOpt, replicationFactorOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnderReplicatedPartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnavailablePartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnavailablePartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicsWithOverridesOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + reportUnavailablePartitionsOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, ifExistsOpt, allTopicLevelOpts -- Set(alterOpt, deleteOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, ifNotExistsOpt, allTopicLevelOpts -- Set(createOpt))

    }
  }

  def askToProceed(): Unit = {
    println("Are you sure you want to continue? [y/n]")
    if (!Console.readLine().equalsIgnoreCase("y")) {
      println("Ending your session")
      Exit.exit(0)
    }
  }

}
