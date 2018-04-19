package org.cboard.influxdb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.BatchPoints.Builder;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.jdbc.internal.jetty.util.StringUtil;


/**
 *  文档：https://docs.influxdata.com/influxdb/v1.5/query_language/functions/
 *  时间单位：w星期，d天，h小时，m分钟，s秒，ms毫秒,u微秒
 * @author qiang123
 *
 */
public class InfluxdbService {
	
	public static boolean openInfluxdb() {
		return true;
	}

	private static final Logger log = LoggerFactory.getLogger(InfluxdbService.class);
	
    private String   database;
    private String   policyName;
    
    public InfluxdbService(String database, InfluxDB influxDB) {
		super();
		this.database = database;
		this.influxDB = influxDB;
	}

	private InfluxDB influxDB;


    /**
     * 创建数据库
     */
    public void createDatabase() {
        influxDB.createDatabase(database);
    }

    /**
     * 创建保存策略 <br/>
     * CREATE RETENTION POLICY "default" ON "influxdb-database" DURATION 30d REPLICATION 1 DEFAULT
     *
     * @param duration 存放时间 (30d)
     * @param replicationNum 备份数量
     */
    public void createRetentionPolicy(String duration, Integer replicationNum) {

        String command = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %s DEFAULT",
                                       policyName, database, duration, replicationNum);

        this.query(command);
    }

    /**
     * 插入数据
     *
     * @param measurement a Point in a fluent manner
     * @param tagsToAdd the Map of tags to add
     * @param fields the fields to add
     */
    public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields,long time) {

    	log.debug("insert "+measurement+"  time="+time); 
    	
    	Builder batchBuilder = BatchPoints.database(database);
        if(tags != null) {
        	for(Entry<String,String> tag:tags.entrySet()) {
        		batchBuilder.tag(tag.getKey(), tag.getValue());
        	}
        }
    	
    	BatchPoints batchPoints = batchBuilder.retentionPolicy(policyName)  
                .consistency(ConsistencyLevel.ALL)  
                .build();  
    	Point.Builder pointBuilder = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS).fields(fields) ;
    	batchPoints.point(pointBuilder.build());  
    	influxDB.write(batchPoints);  

    }
    public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields) {

        this.insert(measurement, tags, fields,System.currentTimeMillis()); 
    }

    /**
     * 查询数据<br>
     * influxdb的time字段是纳秒级时间戳，19位数，在毫秒级别的时间戳上还需要添加6个0
     * @param command
     * @return QueryResult
     */
    public QueryResult query(String command) {
    	log.debug("query-> {}",command);
        return influxDB.query(new Query(command, database));
    }
    
    public List<String> queryColumns(String command) {
    	return getColumns(this.query(command));
    }
    public String[][] getValues(QueryResult queryResult,String timeFormat){
    	if(StringUtil.isNotBlank(queryResult.getError())) {
    		throw new RuntimeException("influxdb query error:"+queryResult.getError());
    	}
    	List<QueryResult.Result> results = queryResult.getResults();
	    
    	List<String[]> list = new LinkedList<>();
        for (QueryResult.Result result : results) {
        	if(result.getSeries() == null) {
        		log.debug("没有series--》error:{}",result.getError());
        		continue;
        	}
        	for(Series s : result.getSeries()) {
        		List<String> columns = s.getColumns();
        		list.add(columns.toArray(new String[columns.size()]));
        		
        		for(List<Object> row : s.getValues()) {//每行
        			String[] v = new String[row.size()];
        			
        			for(int i=0;i<row.size();i++) {
        				v[i] =toStr(i, row.get(i),timeFormat);
        			}
        			list.add(v);
        		}
        		
        	}
            
        }
        return list.toArray(new String[list.size()][]);
    }
    public String toStr(int i,Object o,String format) {
    	if(o == null) {
    		return "";
    	}
    	if(i == 0) {
    		return DateUtils.parseUTC2Str(o.toString(),format);//第一列时间
    	}
    	return o.toString();
    }
    public List<String> getColumns(QueryResult queryResult){
    	if(StringUtil.isNotBlank(queryResult.getError())) {
    		throw new RuntimeException("influxdb query error:"+queryResult.getError());
    	}
    	List<QueryResult.Result> results = queryResult.getResults();
	    
    	List<String> l = new ArrayList<>();
        for (QueryResult.Result result : results) {
        	if(result.getSeries() == null) {
        		log.debug("没有series--》error:{}",result.getError());
        		continue;
        	}
        	
        	for(Series s : result.getSeries()) {
        		List<String> columns = s.getColumns();
        		l.addAll(columns);
        	}
        }
        return l;
    }
    
    public List<String> ListResult(QueryResult queryResult){
    	if(StringUtil.isNotBlank(queryResult.getError())) {
    		throw new RuntimeException("influxdb query error:"+queryResult.getError());
    	}
    	List<QueryResult.Result> results = queryResult.getResults();
	    
        for (QueryResult.Result result : results) {
        	if(result.getSeries() == null) {
        		log.debug("没有series--》error:{}",result.getError());
        		continue;
        	}
        	for(Series s : result.getSeries()) {
        		List<String> columns = s.getColumns();
        		return columns;
        		
        	}
            
        }
        return null;
    }
    
   
    public String getCountSql(String sql,boolean isAppendCountToOut) {
    	
    	String count = "count(*)";
    	sql = sql.replaceAll("\\s{1,}", " ");//将多个空格替换为一个空格
    	sql = sql.replaceAll("order by .+ (desc|asc)", " ");//order by 去掉，因为外面套查询后，会报错
    	
    	return "select "+count+" from (" + sql + ")";
    	/*if(isAppendCountToOut){
    		return "select "+count+" from (" + sql + ")";
    	}else{
    		return sql.replaceAll("\\s{1,}", " ").replaceFirst("select .+ from", "select "+count+" from");
    	}*/
    }
    
    public void logResult(QueryResult queryResult) {
    	System.out.println(queryResult.toString());
    	List<QueryResult.Result> results = queryResult.getResults();
	       
        for (QueryResult.Result result : results) {
        	if(result.getSeries() == null) {
        		System.out.println("没有series--》error:"+result.getError());
        		continue;
        	}
        	for(Series s : result.getSeries()) {
        		System.out.println("Columns list="+s.getColumns());
        		for(List<Object> o : s.getValues()) {
        			Object tm = o.get(1);
            		System.out.println("value list="+o);
        		}
        		
        	}
            
        }
    }

}