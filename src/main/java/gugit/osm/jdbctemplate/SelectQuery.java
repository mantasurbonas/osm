package gugit.osm.jdbctemplate;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import gugit.osm.OSM;

/***
 * a convenient wrapper around JDBC NamedTemplate and gugit OSM (for reading entities from SQL)
 * 
 * NOT THREAD SAFE. disposable.
 * 
 * @author urbonman
 */
public class SelectQuery<T> {

	private NamedParameterJdbcTemplate jdbcTemplate;
	private Class<T> entityClass;
	private OSM osm;
	private RowCallbackHandler rowCallbackHandler;
	
	private String sql;
	private Map<String, Object> queryParams = new HashMap<String, Object>();

	private List<T> result = null;
	
	private static final Logger logger = LogManager.getLogger();
	
	public SelectQuery(NamedParameterJdbcTemplate jdbcTemplate, OSM osm, Class<T> entityClass) {
		this.jdbcTemplate = jdbcTemplate;
		this.osm = osm;
		this.entityClass = entityClass;
		
		if (isPrimitive(entityClass))
			this.rowCallbackHandler = new PrimitiveMapperRowCallbackHandlerImpl();
		else
			this.rowCallbackHandler = new POJOMapperRowCallbackHandlerImpl();
	}
	
	public SelectQuery<T> setSql(final String sql){
		this.sql = sql;
		return this;
	}
	
	public SelectQuery<T> setInt(String paramName, Integer paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}

	public SelectQuery<T> setInts(String paramName, Collection<Integer> paramValues){
		this.queryParams.put(paramName, paramValues);
		return this;
	}
	
	public SelectQuery<T> setString(String paramName, String paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}
	
	public SelectQuery<T> setStrings(String paramName, Collection<String> paramValue){
		this.queryParams.put(paramName, paramValue);
		return this;
	}
	
	public SelectQuery<T> setTimestamp(String paramName, Timestamp paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}

	public SelectQuery<T> setDate(String paramName, Date paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}

	public SelectQuery<T> setBoolean(String paramName, Boolean paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}

	public SelectQuery<T> setDouble(String paramName, Double paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}
	
	private boolean isPrimitive(Class<T> clazz) {
		switch(clazz.getName()){
			case "java.lang.String": 
			case "java.lang.Integer": 
			case "java.lang.Boolean": 
			case "java.lang.Double": 
			case "java.lang.Float": 
			case "java.lang.Byte":
			case "java.lang.Short":
			case "java.lang.Long":
			case "java.lang.Character":
			case "java.math.BigDecimal":
				return true;
		}
		return false;
	}
	
	private class POJOMapperRowCallbackHandlerImpl implements RowCallbackHandler{
		public void processRow(ResultSet rs) throws SQLException {
			result = osm.readEntities(rs, entityClass);
		}
	}

	private class PrimitiveMapperRowCallbackHandlerImpl implements RowCallbackHandler{
		@SuppressWarnings("unchecked")
		public void processRow(ResultSet rs) throws SQLException {
			if (result == null)
				result = new ArrayList<>();
			result.add((T)rs.getObject(1));
		}
	}

	public Collection<T> list() {
		this.result = new ArrayList<T>();
		logger.debug(sql);
		jdbcTemplate.query(sql, queryParams, rowCallbackHandler);
		return result;
	}
	
	public T get(){
		ArrayList<T> res = new ArrayList<T>();
		this.result = res;
		jdbcTemplate.query(sql, queryParams, rowCallbackHandler);
		if (result.size() == 0)
			return null;
		
		if (result.size () == 1)
			return result.get(0);
		
		throw new OsmJdbcException("expected single result, actual size is "+result.size()+" for SQL query "+sql);
	}

}
