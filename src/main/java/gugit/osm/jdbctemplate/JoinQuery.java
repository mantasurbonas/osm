package gugit.osm.jdbctemplate;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import gugit.osm.OSM;

public class JoinQuery<T> {

	private Collection<T> entities;
	private String property;
	private String sql;

	private NamedParameterJdbcTemplate jdbc;
	private OSM osm;
	private RowCallbackHandlerImpl rowCallbackHandler = new RowCallbackHandlerImpl();
	private MapSqlParameterSource queryParams = new MapSqlParameterSource();

	private static final Logger logger = LoggerFactory.getLogger(JoinQuery.class);
	
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
        if (paramValue != null)
            this.queryParams.addValue(paramName, paramValue);
        else
            this.queryParams.addValue(paramName, null, Types.INTEGER);
        return this;
    }

    public JoinQuery<T> setInts(String paramName, Collection<Integer> paramValues){
        this.queryParams.addValue(paramName, paramValues);
        return this;
    }
    
    public JoinQuery<T> setString(String paramName, String paramValue) {
        if (paramValue != null)
            this.queryParams.addValue(paramName, paramValue);
        else
            this.queryParams.addValue(paramName, null, Types.VARCHAR);
        return this;
    }
    
    public JoinQuery<T> setStrings(String paramName, Collection<String> paramValue){
        this.queryParams.addValue(paramName, paramValue);
        return this;
    }
    
    public JoinQuery<T> setTimestamp(String paramName, Timestamp paramValue) {
        if (paramValue != null)
            this.queryParams.addValue(paramName, paramValue);
        else
            this.queryParams.addValue(paramName, null, Types.TIMESTAMP);
        return this;
    }

    public JoinQuery<T> setDate(String paramName, Date paramValue) {
        if (paramValue != null)
            this.queryParams.addValue(paramName, paramValue);
        else
            this.queryParams.addValue(paramName, null, Types.DATE);
        return this;
    }

    public JoinQuery<T> setBoolean(String paramName, Boolean paramValue) {
        if (paramValue != null)
            this.queryParams.addValue(paramName, paramValue);
        else
            this.queryParams.addValue(paramName, null, Types.BOOLEAN);
        return this;
    }

    public JoinQuery<T> setDouble(String paramName, Double paramValue) {
        if (paramValue != null)
            this.queryParams.addValue(paramName, paramValue);
        else
            this.queryParams.addValue(paramName, null, Types.DOUBLE);
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
