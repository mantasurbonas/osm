package gugit.osm.jdbctemplate;

public class OsmJdbcException extends RuntimeException{

	private static final long serialVersionUID = 1L;

	public OsmJdbcException(String msg){
		super(msg);
	}
}
