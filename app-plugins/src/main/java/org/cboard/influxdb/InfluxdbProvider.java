package org.cboard.influxdb;

import org.cboard.dataprovider.DataProvider;
import org.cboard.dataprovider.annotation.DatasourceParameter;
import org.cboard.dataprovider.annotation.ProviderName;
import org.cboard.dataprovider.annotation.QueryParameter;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.jdbc.internal.jetty.util.StringUtil;

/**
 * Created by zyong on 2016/9/27.
 */
// uncomment to active MyDataProvider example
@ProviderName(name = "influxdb")
public class InfluxdbProvider  extends DataProvider {

	private static final Logger log = LoggerFactory.getLogger(InfluxdbProvider.class);
	
    // **数据源管理** 页面 创建数据源的时候需要配置的参数
    @DatasourceParameter(label = "链接", type = DatasourceParameter.Type.Input, order = 1)
    private String url = "url";
    
 // **数据源管理** 页面 创建数据源的时候需要配置的参数
    @DatasourceParameter(label = "账号", type = DatasourceParameter.Type.Input, order = 2)
    private String username = "username";
    
    // **数据源管理** 页面 创建数据源的时候需要配置的参数
    @DatasourceParameter(label = "密码", type = DatasourceParameter.Type.Input, order = 3)
    private String password = "password";
    
    @DatasourceParameter(label = "数据库", type = DatasourceParameter.Type.Input, order = 4)
    private String database = "database";
    
    @QueryParameter(label = "{{'DATAPROVIDER.JDBC.SQLTEXT'|translate}}",
            type = QueryParameter.Type.TextArea,
            required = true,
            order = 1)
    private String SQL = "sql";


    // **图表设计**页面 读取数据接收参数
    @QueryParameter(label = "时间格式", type = QueryParameter.Type.Input
    		,value="yyyy-MM-dd HH:mm:ss"//value没发现有什么用
    		,required = false
    		,placeholder="查询返回的时间格式（默认：yyyy-MM-dd HH:mm:ss）"
    		,order = 1)
    private String time_format = "time_format";

    @Override
    public boolean doAggregationInDataSource() {
        return false;
    }

    /**
     *
     * @return
     * @throws Exception
     */
    @Override
    public String[][] getData() throws Exception {
        
    	InfluxdbService influxdbSvc = this.getInfluxdbService();
    	
    	String sql = query.get(SQL);
    	
    	String time_format = query.get(this.time_format);
    	
    	if(StringUtil.isBlank(time_format)) {
    		time_format = "yyyy-MM-dd HH:mm:ss";
    	}
    	
    	QueryResult queryResult = influxdbSvc.query(sql);
    	
//    	List<String> columns = influxdbSvc.getColumns(queryResult);
//    	queryResult.getResults().stream().forEach(r -> r.getSeries().stream().forEach(a -> a.getValues().stream().forEach(b -> b.stream().);));
//    	
    	
    	
    	return influxdbSvc.getValues(queryResult,time_format);
    }
    public InfluxDB getInfluxDB() {
    	String url = dataSource.get(this.url);
    	String username = dataSource.get(this.username);
    	String password = dataSource.get(this.password);
    	InfluxDB influxDB = InfluxDBFactory.connect(url, username, password);
            
        return influxDB;
    }
    public InfluxdbService getInfluxdbService() {
    	
    	String database = dataSource.get(this.database);
    	
    	return new InfluxdbService(database,this.getInfluxDB());
    }
}
