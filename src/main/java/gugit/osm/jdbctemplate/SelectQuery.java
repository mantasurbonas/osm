package gugit.osm.jdbctemplate;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
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
	private MapSqlParameterSource queryParams = new MapSqlParameterSource();

	private List<T> result = null;
	
	private static final Logger logger = LoggerFactory.getLogger(SelectQuery.class);
	
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
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.INTEGER);
		return this;
	}

	public SelectQuery<T> setInts(String paramName, Collection<Integer> paramValues){
		this.queryParams.addValue(paramName, paramValues);
		return this;
	}
	
	public SelectQuery<T> setString(String paramName, String paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.VARCHAR);
		return this;
	}
	
	public SelectQuery<T> setStrings(String paramName, Collection<String> paramValue){
		this.queryParams.addValue(paramName, paramValue);
		return this;
	}
	
	public SelectQuery<T> setTimestamp(String paramName, Timestamp paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.TIMESTAMP);
		return this;
	}

	public SelectQuery<T> setDate(String paramName, Date paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.DATE);
		return this;
	}

	public SelectQuery<T> setBoolean(String paramName, Boolean paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.BOOLEAN);
		return this;
	}

	public SelectQuery<T> setDouble(String paramName, Double paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.DOUBLE);
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
			case "java.sql.Date":
			case "java.sql.Timestamp":
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

	public List<T> list() {
		this.result = new ArrayList<T>();
		logger.debug(sql);
		jdbcTemplate.query(sql, queryParams, rowCallbackHandler);
		return result;
	}
	
	public T singleResult(){
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
