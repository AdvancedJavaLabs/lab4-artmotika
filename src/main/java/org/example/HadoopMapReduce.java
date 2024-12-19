package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.calculate.SalesInfo;
import org.example.calculate.SalesMapper;
import org.example.calculate.SalesReducer;
import org.example.sort.SortingKeyInfo;
import org.example.sort.SortingKeyMapper;
import org.example.sort.SortingKeyReducer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HadoopMapReduce {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Отключить все логи для определенного логгера
        Logger logger1 = Logger.getLogger("org.apache.hadoop");
        Logger logger2 = Logger.getLogger("org.apache.hadoop.hdfs");
        logger1.setLevel(Level.OFF);
        logger2.setLevel(Level.OFF);

        // Define predefined arguments (input, output, numReducers, blockSize)
        String[][] arguments = {
                {"input1", "output1", "1", "512"},
                {"input2", "output2", "1", "1024"},
                {"input3", "output3", "1", "2048"},
                {"input4", "output4", "1", "4096"},
                {"input5", "output5", "1", "8192"},
                {"input6", "output6", "1", "30000"},
                {"input11", "output11", "1", "512"},
                {"input12", "output12", "2", "512"},
                {"input13", "output13", "4", "512"},
                {"input14", "output14", "8", "512"},
                {"input15", "output15", "16", "512"},
                {"input16", "output16", "32", "512"},
                {"input21", "output21", "1", "1024"},
                {"input22", "output22", "2", "1024"},
                {"input23", "output23", "4", "1024"},
                {"input24", "output24", "8", "1024"},
                {"input25", "output25", "16", "1024"},
                {"input26", "output26", "32", "1024"}
        };

        // Loop through each set of arguments
        for (String[] argSet : arguments) {
            String inputPathString = argSet[0];
            String outputPathString = argSet[1];
            String tempOutputPathString = outputPathString + "-temp";

            Path inputPath = new Path(inputPathString);
            Path outputPath = new Path(outputPathString);
            Path tempOutputPath = new Path(tempOutputPathString);

            int numReducers = Integer.parseInt(argSet[2]);
            int blockSizeBytes = Integer.parseInt(argSet[3]) * 1024;

            // Настройка конфигурации
            Configuration configuration = new Configuration();
            configuration.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(blockSizeBytes));

            // Check and delete existing output directories
            FileSystem fs = FileSystem.get(configuration);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            if (fs.exists(tempOutputPath)) {
                fs.delete(tempOutputPath, true);
            }

            long startTime = System.currentTimeMillis();

            // Создаем задачу для анализа продаж
            Job analysisJob = Job.getInstance(configuration, "Sales Analysis");
            analysisJob.setJarByClass(HadoopMapReduce.class);
            analysisJob.setNumReduceTasks(numReducers);
            analysisJob.setMapperClass(SalesMapper.class);
            analysisJob.setReducerClass(SalesReducer.class);
            analysisJob.setMapOutputKeyClass(Text.class);
            analysisJob.setMapOutputValueClass(SalesInfo.class);
            analysisJob.setOutputKeyClass(Text.class);
            analysisJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(analysisJob, inputPath);
            FileOutputFormat.setOutputPath(analysisJob, tempOutputPath);

            // Запуск первой задачи
            if (!analysisJob.waitForCompletion(true)) {
                System.err.println("Sales analysis job failed for input: " + inputPathString);
                System.exit(1);
            }

            // Создаем задачу для сортировки результатов
            Job sortingJob = Job.getInstance(configuration, "Sort by Revenue");
            sortingJob.setJarByClass(HadoopMapReduce.class);
            sortingJob.setMapperClass(SortingKeyMapper.class);
            sortingJob.setReducerClass(SortingKeyReducer.class);
            sortingJob.setMapOutputKeyClass(DoubleWritable.class);
            sortingJob.setMapOutputValueClass(SortingKeyInfo.class);
            sortingJob.setOutputKeyClass(SortingKeyInfo.class);
            sortingJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(sortingJob, tempOutputPath);
            FileOutputFormat.setOutputPath(sortingJob, outputPath);

            // Запуск второй задачи
            if (!sortingJob.waitForCompletion(true)) {
                System.err.println("Sorting job failed for input: " + inputPathString);
                System.exit(1);
            }

            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            System.out.println("Total execution time for " + inputPathString + ": " + executionTime + " ms.");
        }

        System.exit(0);
    }
}