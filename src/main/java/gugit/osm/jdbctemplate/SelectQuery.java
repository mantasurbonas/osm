package gugit.osm.jdbctemplate;

import gugit.om.mapping.ISerializerRegistry;
import gugit.osm.OSM;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/***
 * a convenient wrapper around JDBC NamedTemplate and gugit OSM (for reading entities from SQL)
 * 
 * NOT THREAD SAFE
 * 
 * @author urbonman
 */
public class SelectQuery<T> {

	private JdbcTemplate jdbcTemplate;
	private Class<T> entityClass;
	private OSM osm;
	
	private String sql;
	private Map<String, Object> queryParams = new HashMap<String, Object>();

	private List<T> result = null;
	private RowCallbackHandlerImpl rowCallbackHandler = new RowCallbackHandlerImpl();
	
	
	public SelectQuery(JdbcTemplate jdbcTemplate, ISerializerRegistry serializers, Class<T> entityClass) {
		this.jdbcTemplate = jdbcTemplate;
		this.osm = new OSM(serializers);
		this.entityClass = entityClass;
	}

	public SelectQuery<T> reset() {
		this.queryParams.clear();
		this.osm.reset();
		this.result = null;
		this.sql = null;
		return this;
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
	
	public List<T> list() {
		new NamedParameterJdbcTemplate(jdbcTemplate)
				.query(sql, queryParams, rowCallbackHandler);
		return result;
	}

}
