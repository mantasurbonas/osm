package gugit.osm.jdbctemplate;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import gugit.osm.OSM;

public class JoinQuery<T> {

	private Collection<T> entities;
	private String property;
	private String sql;

	private NamedParameterJdbcTemplate jdbc;
	private OSM osm;
	private RowCallbackHandlerImpl rowCallbackHandler = new RowCallbackHandlerImpl();
	private Map<String, Object> queryParams = new HashMap<String, Object>();

	private static final Logger logger = LogManager.getLogger();
	
	public JoinQuery(NamedParameterJdbcTemplate namedParameterJdbcTemplate, OSM osm, String propertyName) {
		this.jdbc = namedParameterJdbcTemplate;
		this.osm = osm;
		this.property = propertyName;
	}

	public JoinQuery<T> setEntities(Collection<T> entities) {
		this.entities = entities;
		return this;
	}

	public JoinQuery<T> setSql(String sql) {
		this.sql = sql;
		return this;
	}

	public JoinQuery<T> setInt(String paramName, Integer paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}

	public JoinQuery<T> setString(String paramName, String paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}
	
	public JoinQuery<T> setTimestamp(String paramName, Timestamp paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}

	public JoinQuery<T> setDate(String paramName, Date paramValue) {
		this.queryParams.put(paramName, paramValue);
		return this;
	}
	
	private class RowCallbackHandlerImpl implements RowCallbackHandler{
		public void processRow(ResultSet rs) throws SQLException {
			osm.leftJoin(entities, property, rs);
		}
	}
	
	public void execute(){
		logger.debug(sql);
		jdbc.query(sql, queryParams, rowCallbackHandler);
	}
}
