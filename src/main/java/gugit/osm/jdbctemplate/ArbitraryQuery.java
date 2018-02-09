package gugit.osm.jdbctemplate;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class ArbitraryQuery {

	private NamedParameterJdbcTemplate jdbcTemplate;
	private String sql;
	private MapSqlParameterSource queryParams = new MapSqlParameterSource();

	public ArbitraryQuery(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
		this.jdbcTemplate = namedParameterJdbcTemplate;
	}

	public ArbitraryQuery setSql(String sql) {
		this.sql = sql;
		return this;
	}
	
	public ArbitraryQuery setInt(String paramName, Integer paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.INTEGER);
		return this;
	}

	public ArbitraryQuery setInts(String paramName, Collection<Integer> paramValues){
		this.queryParams.addValue(paramName, paramValues);
		return this;
	}
	
	public ArbitraryQuery setString(String paramName, String paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.VARCHAR);
		return this;
	}
	
	public ArbitraryQuery setStrings(String paramName, Collection<String> paramValue){
		this.queryParams.addValue(paramName, paramValue);
		return this;
	}
	
	public ArbitraryQuery setTimestamp(String paramName, Timestamp paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.TIMESTAMP);
		return this;
	}

	public ArbitraryQuery setDate(String paramName, Date paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.DATE);
		return this;
	}

	public ArbitraryQuery setBoolean(String paramName, Boolean paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.BOOLEAN);
		return this;
	}

	public ArbitraryQuery setDouble(String paramName, Double paramValue) {
		if (paramValue != null)
			this.queryParams.addValue(paramName, paramValue);
		else
			this.queryParams.addValue(paramName, null, Types.DOUBLE);
		return this;
	}
	
	public int update(){
		return jdbcTemplate.update(sql, queryParams);
	}
}
