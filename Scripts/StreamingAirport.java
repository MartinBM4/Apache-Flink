package org.masterinformatica.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.STRING;

public class StreamingAirport {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: StreamingAirport <streaming directory>");
      System.exit(1);
    }
    String path = args[0];

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> data =env.readTextFile(path);
    //filter
    SingleOutputStreamOperator<String> spanish= data.filter(s -> s.split(",")[8].equals("\"ES\""));
    spanish.print();


    env.execute("Airport");
  }

}
