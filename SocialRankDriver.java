package gr.tuc.softnet;

/*******************************************************************************************************************************************/

/********************************		Libraries		************************************************************************************/

/*******************************************************************************************************************************************/


import java.io.*;



import org.apache.hadoop.io.LongWritable;	
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;	

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




import org.apache.hadoop.conf.Configuration;




/*******************************************************************************************************************************************/

/********************************		Main		****************************************************************************************/

/*******************************************************************************************************************************************/



public class SocialRankDriver {
	public static double thres;
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception
	{
		
		try{
			   Configuration conf = new Configuration();
	            conf.addResource(new Path("/home/haduser/hadoop-2.6.0/etc/hadoop/core-site.xml"));
	            FileSystem fs = FileSystem.get(conf);	
	            fs.mkdirs(new Path(args[0]+"hdfs"));
	            fs.copyFromLocalFile(new Path(args[0]),new Path(args[0]+"hdfs"));		//	copy from local filesystem to hdfs
	            init(new Path(args[0]+"hdfs"),new Path(args[2]),Integer.parseInt(args[6]));		// run init job
	            thres=Double.parseDouble(args[5]);	//threshold global variable so i can use it to diff job
	    		boolean again=true;
	    		int times=0;
	    		
	    		while(again){	
	    			iter(new Path(args[2]),new Path(args[3]),Integer.parseInt(args[6]));	//run 3 times iter job 
	    			fs.delete(new Path(args[2]));
	    			iter(new Path(args[3]),new Path(args[2]),Integer.parseInt(args[6]));
	    			fs.delete(new Path(args[3]));
	    			iter(new Path(args[2]),new Path(args[3]),Integer.parseInt(args[6]));
	    			times=times+3;
	    			diff(new Path(args[2]),new Path(args[3]),new Path(args[4]),Integer.parseInt(args[6]));  		//run diff job
	    			for (int i=0;i<Integer.parseInt(args[6]);i++){
	    				FSDataInputStream  fsstream = null;
	    			    if (i<10){
	    			    	fsstream=fs.open(new Path(args[4]+"/part-r-0000"+String.valueOf(i)));	//read the output of diff job
	    			    }
	    			    else if(i<100){
	    			    	fsstream=fs.open(new Path(args[4]+"/part-r-000"+String.valueOf(i)));	//read the output of diff job
	    			    }
	    			    else{
	    			    	fsstream=fs.open(new Path(args[4]+"/part-r-00"+String.valueOf(i)));	//read the output of diff job
	    			    }
	    			   
						BufferedReader in = new BufferedReader(new InputStreamReader(fsstream));
	    				if(in.readLine()==null){
	    					again=false;
	    				}
	    				else{
	    					again=true;
	    					break;
	    				}
	    			}
	    			if(again==true){	//if is empty it means that all the ranks converge , else run 4 times iter job
	    				fs.delete(new Path(args[4]));
	    				fs.delete(new Path(args[2]));
	    				iter(new Path(args[3]),new Path(args[2]),Integer.parseInt(args[6]));
	    				times++;
	    				fs.delete(new Path(args[3]));
	    			}
	    			
	    		}
	    		finish(new Path(args[3]),new Path(args[1]+"hdfs"),Integer.parseInt(args[6]));	//run dfinish job
	            fs.copyToLocalFile(new Path(args[1]+"hdfs"), new Path(args[1]));
	            fs.delete(new Path(args[1]+"hdfs"));
	            fs.delete(new Path(args[3]));
	            fs.delete(new Path(args[2]));
	            fs.delete(new Path(args[0]+"hdfs"));
	            fs.delete(new Path(args[4]));
	          
	           System.out.println("***********************************************************************************************************************************************");
	            System.out.print(times);
	            System.out.println("*****************************************************************************************************************************************");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	
	
/*******************************************************************************************************************************************/
	
/********************************		Run jobs		************************************************************************************/
	
/*******************************************************************************************************************************************/
	

	public static void init(Path InputDir,Path OutputDir,int reducers) throws IOException,InterruptedException, ClassNotFoundException{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "init");
		job.setJarByClass(SocialRankDriver.class);
		
		FileInputFormat.addInputPath(job,InputDir);
		FileOutputFormat.setOutputPath(job,OutputDir);
		
		job.setNumReduceTasks(reducers);
		job.setMapperClass(Map1.class);
		job.setReducerClass(Red1.class);
		
		job.setMapOutputKeyClass(IntWritable.class); 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);	
		
	}
	
	
	public static void iter(Path InputDir,Path OutputDir,int reducers) throws IOException,InterruptedException, ClassNotFoundException{
	
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "iter");
		job.setJarByClass(SocialRankDriver.class);
		
		FileInputFormat.addInputPath(job,InputDir);
		FileOutputFormat.setOutputPath(job,OutputDir);
		
		job.setNumReduceTasks(reducers);
		job.setMapperClass(Map2.class);
		job.setReducerClass(Red2.class);
		
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);	
		
	
	}
	
	public static void diff(Path InputDir,Path InputDir1,Path OutputDir,int reducers) throws IOException,InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "diff");
	    job.setJarByClass(SocialRankDriver.class);
	    
		FileInputFormat.addInputPath(job,InputDir);
		FileInputFormat.addInputPath(job,InputDir1);
		FileOutputFormat.setOutputPath(job,OutputDir);
		
		job.setNumReduceTasks(reducers);
		job.setMapperClass(Map3.class);
		job.setReducerClass(Red3.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		job.waitForCompletion(true);	
	    
	    
	}
	
	
	public static void finish(Path InputDir,Path OutputDir,int reducers) throws IOException,InterruptedException, ClassNotFoundException{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "finish");
		job.setJarByClass(SocialRankDriver.class);
		
		FileInputFormat.addInputPath(job,InputDir);
		FileOutputFormat.setOutputPath(job,OutputDir);
		
		job.setNumReduceTasks(reducers);
		job.setMapperClass(Map4.class);
		job.setReducerClass(Red4.class);
		job.setPartitionerClass(Partbyrank.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class); 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.waitForCompletion(true);	
		
	
	}
	
	
	
	/*******************************************************************************************************************************************/
	
	/********************************		Mappers and Reducers		************************************************************************/
	
	/*******************************************************************************************************************************************/
	
	
	
	public static class Map1 extends Mapper<LongWritable,Text,IntWritable,IntWritable>{ //input follower following
		
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String[] tokens = value.toString().split("\\s+"); //split by whitespace
			if(tokens.length>1){
				String fkey=tokens[0];
				String fvalue=tokens[1];
				if(fkey.matches("[0-9]+") && fvalue.matches("[0-9]+") ){   //only 2 integers tokens
					context.write(new IntWritable(Integer.parseInt(fkey)),new IntWritable(Integer.parseInt(fvalue)));
				}
			}
		}
		
	}
	
	public static class Red1 extends Reducer<IntWritable,IntWritable,IntWritable, Text>{ // output follower rankfollower following,following,...
		
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			
			String valout="";
			for (IntWritable value : values) 
			{
				valout = valout.concat(value.toString().concat(",")); 
			}
			valout=valout.substring(0,valout.length()-1);  
			context.write(key, new Text("#1# "+valout)); 

		}

	}

	
	public static class Map2 extends Mapper<LongWritable,Text,Text,Text>{ // output format: follower #follows: following,following,... 
																		  //              : following #rank:rankfollower!outer:numof(following) 
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String[] tokens = value.toString().split("#");
			
				String follower=tokens[0];
				String rankfollower=tokens[1];
				String following[]=tokens[2].split(",");
				
				String allOuter= "#follows:"+tokens[2].replaceAll("\\s","");
				
				context.write(new Text(follower.replaceAll("\\s","")),new Text(allOuter.replace("\\s","")));
				
				
				for( int i=0;i<following.length;i++){
					context.write(new Text(following[i].replaceAll("\\s","")),new Text("#rank:"+rankfollower+"!outer:"+Integer.toString(following.length)));
				}
			
			
			
			
		}
		
	}
	
	public static class Red2 extends Reducer<Text,Text, Text, Text>{  //compute new rank 
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			double newrank=0.15;
			double rankfol=0;
			String outer="";
			for (Text value : values) 
			{
				if(value.toString().substring(0,5).equals("#rank")){
					String[] rkandouter=value.toString().split("!");
					String[] rkval=rkandouter[0].toString().split(":");
					String[] outerval=rkandouter[1].toString().split(":");
					rankfol=rankfol+(Double.parseDouble(rkval[1])/Integer.parseInt(outerval[1]));
				}
				if(value.toString().substring(0,8).equals("#follows")){
					outer=value.toString().substring(9);
				}
			}
			String nrk=String.valueOf(newrank+(1-newrank)*rankfol);
			if(key.toString().matches("[0-9]+")){
				context.write(new Text(key),new Text("#"+nrk+"# "+outer));
			}
		
			
		}
	}


public static class Map3 extends Mapper<LongWritable,Text,IntWritable,DoubleWritable>{ 
	
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		String[] tokens = value.toString().split("#");
		String idnode=tokens[0].replaceAll("\\s","");
		String rank=tokens[1];
		if(idnode.matches("[0-9]+") && rank.matches("[0-9]+(\\.[0-9]+)?")){
			context.write(new IntWritable(Integer.parseInt(idnode)),new DoubleWritable(Double.parseDouble(rank)));
		
		}
	}
	
}




public static class Red3 extends Reducer<IntWritable,DoubleWritable,Text,Text>{
	
	boolean Stop=true;
	
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		if(Stop){
			double[] rank = new double[2];
			int i=0;
			for (DoubleWritable value : values) 
			{
				rank[i]=value.get();
				i++;
			
			}
			System.out.printf("pair: %f  %f\n",rank[0],rank[1]);
			if(Math.abs(rank[1]-rank[0])>thres){
				Stop=false;
				context.write(new Text("Do it again"),new Text(""));
			}
		}
	}

}

public static class Map4 extends Mapper<LongWritable,Text,DoubleWritable,IntWritable>{ //key LongWritable represents the offset location of the current line being read from the Input Split of the given input file. Text represents the actual current line itself.
	
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		String[] tokens = value.toString().split("#");
		String idnode=tokens[0].replaceAll("\\s","");
		String rank=tokens[1];
		if(idnode.matches("[0-9]+") && rank.matches("[0-9]+(\\.[0-9]+)?")){
			context.write(new DoubleWritable(Double.parseDouble("-"+rank)),new IntWritable(Integer.parseInt(idnode)));
		
		}
	}
	
}
 
public static class Partbyrank extends Partitioner<DoubleWritable,IntWritable>{
	@Override
	public int getPartition(DoubleWritable key, IntWritable value, int numReduceTasks)
    {
		
		double range=(double)1/(double)numReduceTasks;
		double  start=range+(double)0.1;
       for(int i=0;i<numReduceTasks;i++)
       {
    	   
    	   if(-key.get()<start){
    			   return i ;	   
    	   }
    	   else{
    		   start=start+range;
    	   }
       }
       return numReduceTasks-1;
    }
	
}

public static class Red4 extends Reducer<DoubleWritable,IntWritable,IntWritable,DoubleWritable>{
	
	
	
	public void reduce(DoubleWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		for (IntWritable value : values) {
		double newrank=-key.get();
		context.write(value,new DoubleWritable(newrank));
		}
	}

}


}
