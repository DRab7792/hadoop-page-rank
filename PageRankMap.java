package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

// each map task handles one line within an adjacency matrix file
// key: file offset
// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, org.w3c.dom.Text value, Context context)
		throws IOException, InterruptedException {
			int numUrls = context.getConfiguration().getInt("numUrls",1);
			String line = value.toString();
			StringBuffer sb = new StringBuffer();
			// instance an object that records the information for one webpage
			RankRecord rrd = new RankRecord(line);
			int sourceUrl, targetUrl;
			// double rankValueOfSrcUrl;
			if (rrd.targetUrlsList.size()<=0){
				// there is no out degree for this webpage; 
				// scatter its rank value to all other urls
				double rankValuePerUrl = rrd.rankValue/(double)numUrls;
				for (int i=0;i<numUrls;i++){
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
				}
			} else {
				/*Write your code here*/
				//Calculate the partial rank here
				double rankPerOutLink = rrd.rankValue / (double)rrd.targetUrlsList.size();
				//Iterate through outgoing links
				for (Integer cur : rrd.targetUrlsList){
					long curUrl = cur;
					LongWritable outUrl = new LongWritable(curUrl);
					Text outVal = new Text(String.valueOf(rankPerOutLink));
					//Write key value pair for each outgoing link with previously calculated partial rank
					context.write(outUrl, outVal);
					//Append the outgoing link to the current url value to be written
					sb.append("#" + curUrl);
				}
				//Move context write statement inside if to account for the multiple scenarios
				context.write(new LongWritable(rrd.sourceUrl), new Text(sb.toString()));	
			} 
		} // end map

}
