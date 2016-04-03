package gugit.osm.jdbctemplate;

import gugit.om.mapping.WriteBatch;
import gugit.om.mapping.WritePacket;
import gugit.om.mapping.WritePacketElement;
import gugit.osm.OSM;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

/***
 * a convenient wrapper around JDBC NamedTemplate and gugit OSM (for writing entities to SQL)
 * 
 * @author urbonman
 */
@Service
@Scope("prototype")
public class PersistanceService implements SqlStatementRegistry{

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	@Autowired
	private OSM osm;
	
	private SqlStatementRegistryImpl sqls = new SqlStatementRegistryImpl();
	
	
	private static boolean debugSQL = false;
	
	public <E> void persist(E rootEntity){
		persistWriteBatch(osm.writeEntity(rootEntity));
	}

	public <E> void persist(Collection<E> entities){
		persistWriteBatch(osm.writeEntities(entities));
	}
	
	public <T> SelectQuery<T> query(final String sql, Class<T> entityClass){
		return new SelectQuery<T>(namedParameterJdbcTemplate, osm, entityClass).setSql(sql);
	}
	
	
	private void persistWriteBatch(WriteBatch batch) {
		SqlParameterSourceAdapter paramSourceAdapter = new SqlParameterSourceAdapter();
		
		WritePacket writePacket;
		while ((writePacket= batch.getNext()) != null){
			if (writePacket.getIdElement().value == null)
				performInsert(writePacket, paramSourceAdapter.wrap(writePacket));
			else
				performUpdate(writePacket, paramSourceAdapter.wrap(writePacket));
		}
	}
	
	private void performUpdate(WritePacket writePacket, SqlParameterSource paramSource) {
		String updateSql = sqls.getUpdateSql(writePacket);
		
		if (debugSQL)
			System.out.println(updateSql );
		
		namedParameterJdbcTemplate.update(updateSql, paramSource);
	}

	private void performInsert(WritePacket writePacket, SqlParameterSource paramSource) {				
		String insertSql = sqls.getInsertSql(writePacket);
		
		if (debugSQL)
			System.out.println(insertSql);
		
		GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
		namedParameterJdbcTemplate.update(insertSql, paramSource, keyHolder);
		updatePacketAfterDBInsert(writePacket, keyHolder);
	}
	
	private static void updatePacketAfterDBInsert(WritePacket writePacket, KeyHolder keyHolder){
		WritePacketElement idElement = writePacket.getIdElement();
		
		Object idVal = findIDValue(idElement.columnName, keyHolder.getKeyList());
		
		if (idVal == null){
			System.out.println("WARNING: "
							+ "could not find ID ('"+idElement.columnName+"') "
							+ "in the keylist "+keyHolder.getKeyList()+". "
							+ "Did the insert succeed?");
		}
		else
			writePacket.updateIDValue(idVal);		
		
		// TODO: update other properties as well
	}
	
	private static Object findIDValue(String idColumnName, List<Map<String, Object>> keyList){
		String needle = idColumnName;
		
		for (Map<String, Object> keys: keyList)
			if (keys.containsKey(needle))
				return keys.get(needle);
		
		String noQuotes = needle.replace("\"", "");
		
		if (!noQuotes.equals(needle))
			for (Map<String, Object> keys: keyList)
				if (keys.containsKey(noQuotes))
					return keys.get(noQuotes);
		
		for (Map<String, Object> keys: keyList)
			for (String s: keys.keySet())
				if (s.equalsIgnoreCase(noQuotes))
					return keys.get(s);

		return null;
	}

	@Override
	public void register(Class<?> type) {
		sqls.register(type);
		osm.registerType(type);
	}

	@Override
	public void register(Class<?> type, String insertSQL, String updateSQL) {
		sqls.register(type, insertSQL, updateSQL);
		osm.registerType(type);
	}

	@Override
	public String getUpdateSql(WritePacket writePacket) {
		return sqls.getUpdateSql(writePacket);
	}

	@Override
	public String getInsertSql(WritePacket writePacket) {
		return sqls.getInsertSql(writePacket);
	}

}
