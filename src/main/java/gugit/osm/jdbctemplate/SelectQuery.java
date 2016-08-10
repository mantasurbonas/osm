package gugit.osm.jdbctemplate;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
	private RowCallbackHandlerImpl rowCallbackHandler = new RowCallbackHandlerImpl();
	
	private String sql;
	private Map<String, Object> queryParams = new HashMap<String, Object>();

	private Collection<T> result = null;
	
	private static final Logger logger = LogManager.getLogger();
	
	public SelectQuery(NamedParameterJdbcTemplate jdbcTemplate, OSM osm, Class<T> entityClass) {
		this.jdbcTemplate = jdbcTemplate;
		this.osm = osm;
		this.entityClass = entityClass;
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
	
	private class RowCallbackHandlerImpl implements RowCallbackHandler{
		public void processRow(ResultSet rs) throws SQLException {
			result = osm.readEntities(rs, entityClass);
		}
	}
	
	public Collection<T> list() {
		this.result = new ArrayList<T>();
		logger.debug(sql);
		jdbcTemplate.query(sql, queryParams, rowCallbackHandler);
		return result;
	}

}
