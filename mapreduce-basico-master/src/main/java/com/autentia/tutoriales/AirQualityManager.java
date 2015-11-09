package com.autentia.tutoriales;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AirQualityManager extends Configured implements Tool {

	public static class AirQualityMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		private static final String SEPARATOR = ";";

		/**
		 * DIA; CO (mg/m3);NO (ug/m3);NO2 (ug/m3);O3 (ug/m3);PM10 (ug/m3);SH2
		 * (ug/m3);PM25 (ug/m3);PST (ug/m3);SO2 (ug/m3);PROVINCIA;ESTACIÓN <br>
		 * 01/01/1997; 1.2; 12; 33; 63; 56; ; ; ; 19 ;ÁVILA ;Ávila
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			final String[] values = value.toString().split(SEPARATOR);

			// Columna 4 para nombre y columna 9 para sueldo antes province 10 y
			// co 1
			final String cedula = format(values[2]).replaceAll("\"", "").trim();

			Long doc;
			if (NumberUtils.isNumber(cedula.toString())) {
				doc = Long.valueOf(cedula);

				final String[] apellidos = format(values[3])
						.replaceAll("\"", "").trim().split(" ");

				if (apellidos.length == 2) {
					Text relacion = null;
					if (apellido1.trim().compareTo(apellidos[0].trim()) == 0
							&& documento > doc)
						relacion = new Text("Padre " + cedula);

					if (apellido2.trim().compareTo(apellidos[0].trim()) == 0
							&& documento > doc)
						relacion = new Text("Madre " + cedula);

					if (apellido1.trim().compareTo(apellidos[0].trim()) == 0
							&& apellido2.trim().compareTo(apellidos[1].trim()) == 0)
						relacion = new Text("Hermano " + cedula);
					if (relacion != null
							&& NumberUtils.isNumber(cedula.toString()))
						context.write(new Text(relacion), new DoubleWritable(
								NumberUtils.toDouble(cedula)));
				}
			}
		}

		private String format(String value) {
			return value.trim();
		}
	}

	public static class AirQualityReducer extends
			Reducer<Text, DoubleWritable, Text, Text> {

		private final DecimalFormat decimalFormat = new DecimalFormat("#.##");

		public void reduce(Text key, Iterable<DoubleWritable> coValues,
				Context context) throws IOException, InterruptedException {
			int measures = 0;
			double totalCo = 0.0f;
			File f = new File("ouput/" + key.toString().split(" ")[0]);
			FileWriter w = new FileWriter(f, true);
			BufferedWriter bw = new BufferedWriter(w);
			PrintWriter wr = new PrintWriter(bw);

			wr.append(Integer.valueOf(
					Double.valueOf(key.toString().split(" ")[1].toString()).intValue())
					.toString() + "\n");
			
			wr.close();
			bw.close();
			
			for (DoubleWritable coValue : coValues) {
				measures++;
			}
			

			if (measures > 0) {
				context.write(key, new Text(decimalFormat.format(measures)));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err
					.println("AirQualityManager required params: {input file} {output dir}");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf());
		job.setJarByClass(AirQualityManager.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(AirQualityMapper.class);
		job.setReducerClass(AirQualityReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static String apellido1;

	public static String apellido2;

	public static Long documento;

	public static void main(String[] args) throws Exception {
		FileReader f = new FileReader("input/persona.txt");
		BufferedReader b = new BufferedReader(f);
		apellido1 = b.readLine();
		apellido2 = b.readLine();
		documento = Long.valueOf(b.readLine());
		b.close();
		ToolRunner.run(new AirQualityManager(), args);
	}
}
