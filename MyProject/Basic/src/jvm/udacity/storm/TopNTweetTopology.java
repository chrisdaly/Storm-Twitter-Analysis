package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import udacity.storm.spout.RandomSentenceSpout;

class TopNTweetTopology
{
  public static void main(String[] args) throws Exception
  {
    // Variable TOP_N number of words.
    int TOP_N = 10;

    // Create the topology.
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    // Twitter dev credentials.
    TweetSpout tweetSpout = new TweetSpout(
      "HlyvgZrpXedi88asKUQb8CG3h",
      "e2grhWo1Ny5fkVXfhoBBWk35suSOFuk0lYdjiuqL3W4RPC4IK0",
      "3013484314-31ZbAYf8DsH6tEeuIHy2jwKBx5puKBraX6tVe2G",
      "Vz8GxVUZomW2fV41W1hRLivH2NisrmsFVpzvNfTMdfpcO"
    );

    // Attach the tweet spout to the topology.
    builder.setSpout("tweet-spout", tweetSpout, 1);
    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");
    builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
    builder.setBolt("intermediate-ranker", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("count-bolt", new Fields("word"));
    builder.setBolt("total-ranker", new TotalRankingsBolt(TOP_N)).globalGrouping("intermediate-ranker");
    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("total-ranker");

    // Create the default config object.
    Config conf = new Config();

    // Set the config in debugging mode.
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // Run it in a live cluster.
      // Set the number of workers for running all spout and bolt tasks.
      conf.setNumWorkers(3);

      // Create the topology and submit with config.
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // Run it in a simulated local cluster.
      // Set the number of threads to run - similar to setting number of workers in live cluster.
      conf.setMaxTaskParallelism(3);

      // Create the local cluster instance.
      LocalCluster cluster = new LocalCluster();

      // Submit the topology to the local cluster.
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // Let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000);

      // Now kill the topology.
      cluster.killTopology("tweet-word-count");

      // Finished so shutdown the local cluster.
      cluster.shutdown();
    }
  }
}
