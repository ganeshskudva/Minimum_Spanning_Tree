package ganzz.apache.hadoop;


import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class MST {
	
	private static final boolean debug = true;
	private static int totalNodes,k;
	private static int RAM_size = 52428800; //RAM size in bytes available on each reducer. 5120 = 5Kb
	private static int data_struct_mem = 50; //Maximum size in bytes required at each reducer to process an individual edge
	private static int max_mem_reducer = 0;
	private static enum TotalEdgeCount {EdgCnt, TotalWeight};
	
	public static class MSTMapper extends Mapper <Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text srcDestPair = new Text();
			Configuration conf = context.getConfiguration();
			String []inputTokens = value.toString().split("\t");
				
		/*		String weight = inputTokens[2];
				int wt = Integer.parseInt(weight);
				
				IntWritable Iwt = new IntWritable(wt); */
				
			int i,j,grp=1,count=0, total_grps;
			int total = Integer.parseInt(conf.get("total"));
			int k = Integer.parseInt(conf.get("k"));
			int u,v,max_grps;
			float temp =0;
			double size =0;
			
			if(k==1)
				max_grps = 1;
			else
				max_grps = (k*(k-1))/2;
			
	
			boolean flag = true;
			IntWritable Iwt = null;
			
			
			System.out.println("In MSTMApper Total is "+ total);
			temp = (float)total/k;
			size = Math.ceil(temp);
			
			Multimap<Integer,Integer> myMMap = ArrayListMultimap.create();
			Multimap<Integer, Integer> FMap = ArrayListMultimap.create();
				
			if(k>1)
			{
			for(i=0; i<total; i++)
			{
				if(size == count)
				{
					grp++;
					count = 0;
				}
				
				
				myMMap.put(grp, i);
				count++;
			}
			
			total_grps = grp;
			grp =1;
			
			for(i=1; i<=max_grps ; i++)
			{
				for(j=i+1; j<=total_grps; j++)
				{
					FMap.putAll(grp, myMMap.get(i));
					FMap.putAll(grp, myMMap.get(j));
					grp++;
				}
					
			}
			
			u = Integer.parseInt(inputTokens[0]);
			v = Integer.parseInt(inputTokens[1]);
			
			for(i=1; i<=max_grps; i++)
			{
				if(FMap.containsEntry(i, u) && FMap.containsEntry(i, v))
				{
					Iwt = new IntWritable(i);
					
					break;
				}
			} 
			}
	//		System.out.println("Within MSTMapper "+context.getWorkingDirectory());
			else
				Iwt = new IntWritable(1);
		//	Text tt = new Text(Iwt.toString());
		//	srcDestPair.set(inputTokens[0] + " " + inputTokens[1]+ " " + inputTokens[2]);
			srcDestPair.set(inputTokens[0] + " " + inputTokens[1]+ " " + inputTokens[2]);
	//		Edge e = new Edge(Integer.parseInt(inputTokens[0].trim()), Integer.parseInt(inputTokens[1].trim()), Integer.parseInt(inputTokens[2].trim()));
			if(context != null)
			{
				System.out.println("In MSTMapper ctxt is not NULL Iwt is "+Iwt+" SrcDestPair is "+srcDestPair);
				context.write(Iwt, srcDestPair);
			}
			else
				System.out.println("In MSTMapper: Ctxt NULL");
			
		}
	}
	
	public static class MSTPrepMapper extends Mapper <Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			Configuration conf = context.getConfiguration();
			System.out.println("Within MSTPrepMapper");
			Text SrcDestPair = new Text();
			long total_weight =0;
			Set<Edge> set = new TreeSet<Edge>();
		
			Integer total_edges = 0;
			
			String[] inputTokens = value.toString().split("\t");
			
			int u = Integer.parseInt(inputTokens[0]);
			int v = Integer.parseInt(inputTokens[1]);
			double wt = Double.parseDouble(inputTokens[2]);
			
			Edge e = new Edge(u,v,wt);
			set.add(e);
			
			total_edges = set.size();
			context.getCounter(TotalEdgeCount.EdgCnt).increment(total_edges);
			Iterator it = set.iterator();
			
			
			while(it.hasNext())
			{
				Edge ed = (Edge) it.next();
				int ued = ed.either();
				int ved = ed.other(ued);
				double wgt = ed.weight();
				total_weight += wgt;
				IntWritable iwt = new IntWritable(ued);
				System.out.println(Integer.toString(ued)+ " "+Integer.toString(ved)+" "+Double.toString(wgt));
				SrcDestPair.set(Integer.toString(ved)+"\t"+Double.toString(wgt));
				
				context.write(iwt, SrcDestPair);
			}
			context.getCounter(TotalEdgeCount.TotalWeight).increment(total_weight);
		}
	}
	
	public static class MSTReducer extends Reducer <IntWritable, Text, IntWritable, Text> {
		
		@Override
		public void reduce(IntWritable IpKey, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			
			Set<Integer> set = new TreeSet<Integer>();
			Text srcDestPair = new Text();
			ArrayList<String> cache = new ArrayList<String>();
			System.out.println("BOND LOGS" );
/*		for(Text val : value)
			{
			String []inputTokens = val.toString().split(" ");
			System.out.println("BOND LOGS"+Integer.parseInt(inputTokens[0].trim())+""+Integer.parseInt(inputTokens[1].trim())+"" +Integer.parseInt(inputTokens[2].trim()));
			Edge e = new Edge(Integer.parseInt(inputTokens[0].trim()), Integer.parseInt(inputTokens[1].trim()), Integer.parseInt(inputTokens[2].trim()));
			context.write(IpKey,e);
			}  */
			/*Count Number of Nodes in the given graph*/
			/*Count Number of Nodes in the given graph*/
			System.out.println("BOND LOGS-2" );
			for(Text val : value) 
			{
				
				String []Tokens = val.toString().split(" ");
				set.add(Integer.parseInt(Tokens[0]));
	    		set.add(Integer.parseInt(Tokens[1]));
	    		
	    		System.out.println("GANESH LOGS-0 "+val.toString());
	    		cache.add(val.toString());
	 //   		System.out.println("GANESH LOGS-1 "+Integer.parseInt(Tokens[0])+" "+ Integer.parseInt(Tokens[1])+" "+ Double.parseDouble(Tokens[2]));
			}
			
			System.out.println("BOND LOGS-3 set size is "+ set.size() +" cache size is  "+cache.size() );
			EdgeWeightedGraph G = new EdgeWeightedGraph(set.size());
			
			Iterator it = set.iterator();
			for(int i=0; i<set.size(); i++)
				G.MapAdd(it.next(), i);
			
			for(int i=0;i < cache.size(); i++)
			{
				String values = cache.get(i);
				System.out.println("GANESH LOGS-2 "+values.toString());
				String[] ipTokens = values.split(" ");
	//			System.out.println("GANESH LOGS-3 "+Integer.parseInt(ipTokens[0])+" "+ Integer.parseInt(ipTokens[1])+" "+ Double.parseDouble(ipTokens[2]));
				Edge e = new Edge(Integer.parseInt(ipTokens[0]), Integer.parseInt(ipTokens[1]), Double.parseDouble(ipTokens[2]));
				G.addEdge(e);
			}
			
			System.out.println("BOND LOGS-4" );
			BoruvkaMST mst = new BoruvkaMST(G);
			for(Edge edg : mst.edges())
			{
				System.out.println("GANESH LOGS-4 "+edg.either() + " "+ edg.other(edg.either()) + " " + edg.weight());
				int v = edg.either();
				IntWritable Iwt = new IntWritable(v);
				
				int u = edg.other(v);
				double wgt = edg.weight();
				srcDestPair.set(Integer.toString(u)+"\t"+Double.toString(wgt));
				context.write(Iwt, srcDestPair);
			} 
		} 
		
		 
		/*Count Number of Nodes in the given graph*/
		

	}
	
	public static void runJob(Configuration conf, Job job, int total_reducers, String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException
	{
			job.setJarByClass(MST.class);
		    job.setMapperClass(MSTMapper.class);
	  //    job.setCombinerClass(MSTReducer.class);
		   
		    if(total_reducers != 0)
		    {
		    	job.setNumReduceTasks(total_reducers);
		    	job.setReducerClass(MSTReducer.class);
		    }
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job, new Path(outputPath));

		    job.waitForCompletion(true);
	}
	
	public static int calc_k_val(long total_edges)
	{
		int k = 0;
		float mult;
		double temp;
		
		mult = (float)(total_edges*data_struct_mem)/RAM_size;
		temp =  Math.ceil(mult);
		
		double disc = 0;
		double disc_sqrt=0;
		disc = 1+(4*temp*2);
		disc_sqrt = Math.sqrt(disc);
		
		if(disc_sqrt > 0)
		{
			k = (int) ((disc_sqrt+1)/2);
		}
		return k;
	}
	
	public static void main(String[] args) throws Exception {
		
		int total=0,total_reducers=0,count=0,k=3,input_edg_cnt=0;
		long total_edges=0;
		String input = null, output = null;
		
		String inputPath = args[0];
		String outputPath = args[1];
		total = Integer.parseInt(args[2]);
		total_edges = Integer.parseInt(args[3]);
		totalNodes = Integer.parseInt(args[2]);
		k = 3;
		
		if (args.length != 4) {
		      System.err.println("Usage: mst <in> <out> <No of nodes> <No of edges>");
		      System.exit(2);
		    }
		
		max_mem_reducer = data_struct_mem * (totalNodes -1);
		
		if(max_mem_reducer > RAM_size) //Each reducer should have atleast enough memory to hold the entire MST of the graph
		{
			System.out.println("RAM size too small on reducers to process input graph");
			System.out.println("RAM size is "+RAM_size+" max_mem_reducer is "+max_mem_reducer);
			System.exit(0);
		}
		
		//Calculate the value of k to begin with
		if(total_edges*data_struct_mem < RAM_size)
			k = 1;
		else
			k = calc_k_val(total_edges);
		
	
		
		do
		{
		
		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(k==1)
		{
			conf.setInt("k", k);
			total_reducers = k;
		}
		else
		{
			conf.setInt("k", k);
			total_reducers = ((k*(k-1))/2);
		}
		
		count++;
		System.out.println("In main k = "+k+" Total Edges is "+total_edges);
		System.out.println("Total in Main is "+total);
		conf.setInt("total", total); //Pass total no of nodes to mapper
		
		
		System.out.println("In Main InputPath is "+inputPath+" OutputPath is "+outputPath);
		
//		runJob(conf, job, total_reducers, inputPath, outputPath);
		
		Job job = new Job(conf, "MST");
		
	
		job.setJarByClass(MST.class);
	    job.setMapperClass(MSTMapper.class);
  //    job.setCombinerClass(MSTReducer.class);
	   
	    job.setNumReduceTasks(total_reducers);
	    job.setReducerClass(MSTReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    System.out.println("In Main FIFormat "+FileInputFormat.getInputPaths(job).length+" Contents "+FileInputFormat.getInputPaths(job));
	    System.out.println("In Main FOFormat "+FileOutputFormat.getOutputPath(job));
	    	    
	    job.waitForCompletion(true); 
//	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	//    job.killJob();
		
	    Job Njob = new Job();
	    if(job.isSuccessful())
	    {
	    	input = outputPath;
	    	output = outputPath+"-"+count;
	    
	    System.out.println("input is "+input +" output is "+output);
	    
//	    Configuration Nconf = new Configuration();
	
//	    runJob(conf, Njob,0,input,output );
	   

	    Njob.setJarByClass(MST.class);
	    Njob.setMapperClass(MSTPrepMapper.class);
//	    Njob.setReducerClass(MSTReducer.class);
	    Njob.setOutputKeyClass(IntWritable.class);
	    Njob.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(Njob, new Path(input));
	    FileOutputFormat.setOutputPath(Njob, new Path(output)); 
	    
	  
//	 	System.exit(Njob.waitForCompletion(true) ? 0 : 1); 
	    }
	    if(Njob.waitForCompletion(true))
	    {
	    	System.out.println();
	    	Counters counter = Njob.getCounters();
	    	Counter c1 = counter.findCounter(TotalEdgeCount.EdgCnt);
	    	Counter c2 = counter.findCounter(TotalEdgeCount.TotalWeight);
	    	total_edges = c1.getValue();
	    	System.out.println("In main val of "+c1.getName()+" is "+c1.getValue());
	    	System.out.println("In main val of "+c2.getName()+" is "+c2.getValue());
	    }
	    
	    if(total_edges > (totalNodes -1))
	    {
	    	count++;
	    	k = calc_k_val(total_edges);
	 //   	total_reducers = 1;
	    	inputPath = output;
	    	if(k == 1)
	    		outputPath = output+"FINAL";
	    	else
	    		outputPath = output+"-"+count;
	    }
	    
		}while(total_edges != (totalNodes - 1));
		
	}
	
}
