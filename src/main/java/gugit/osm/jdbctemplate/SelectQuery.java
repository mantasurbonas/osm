package gugit.osm.jdbctemplate;

import gugit.osm.OSM;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

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

	public SelectQuery<T> setString(String paramName, String paramValue) {
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
		
	private class RowCallbackHandlerImpl implements RowCallbackHandler{
		public void processRow(ResultSet rs) throws SQLException {
			result = osm.readEntities(rs, entityClass);
		}
	}
	
	public Collection<T> list() {
		this.result = new LinkedList<T>();
		jdbcTemplate.query(sql, queryParams, rowCallbackHandler);
		return result;
	}

}
