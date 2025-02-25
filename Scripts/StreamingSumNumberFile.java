package org.masterinformatica.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class StreamingSumNumberFile {

  public static void main(String [] args) throws Exception{
    if (args.length != 1) {
      throw new Exception(
              "Invalid number of arguments. "
                      + "Usage: java org.uma.jmetalsp.externalsources.CounterProvider directory frequency");
    }

    String path = args[0];

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

    Path filePath = new Path(path);
    TextInputFormat inputFormat = new TextInputFormat(filePath);
    inputFormat.setFilesFilter(FilePathFilter.createDefaultFilter());
    DataStreamSource<String> data =env.readFile(inputFormat,path,
        FileProcessingMode.PROCESS_CONTINUOUSLY,8000);

    data.flatMap(new SplitterToSum()).windowAll(TumblingEventTimeWindows.of(Time.seconds(30))).
        apply(new AllWindowFunction<Tuple2<Integer, Integer>, Object, TimeWindow>() {
          @Override
          public void apply(TimeWindow timeWindow, Iterable<Tuple2<Integer, Integer>> iterable,
              Collector<Object> collector) throws Exception {
            int sum=0;
            for (Tuple2<Integer,Integer> tuple:iterable){
              System.out.println(tuple.f1);
              sum+=tuple.f1;
            }
            collector.collect(sum);
          }
        }).print();



    env.execute("Read File");
  }
  public static class SplitterToSum implements FlatMapFunction<String, Tuple2<Integer, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<Integer, Integer>> out) throws Exception {
      String[] aux= sentence.split(",");
        out.collect(new Tuple2<Integer, Integer>(Integer.parseInt(aux[0]), Integer.parseInt(aux[0])));
    }
  }

}