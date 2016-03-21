package gugit.osm.jdbctemplate;

import gugit.om.mapping.ISerializerRegistry;
import gugit.om.mapping.WriteBatch;
import gugit.om.mapping.WritePacket;
import gugit.om.mapping.WritePacketElement;
import gugit.om.metadata.EntityMetadataService;
import gugit.osm.OSM;
import gugit.osm.utils.SQLBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/***
 * a convenient wrapper around JDBC NamedTemplate and gugit OSM (for writing entities to SQL)
 * 
 * NOT THREAD SAFE

 * @author urbonman
 */
public class PersistQuery<E> {

	private NamedParameterJdbcTemplate jdbcTemplate;
	private OSM osm;
	private String tableName;
	
	private SqlParameterSourceAdapter paramSourceAdapter = new SqlParameterSourceAdapter();
	
	private static class PersistSQL{
		public String insertSQL;
		public String updateSQL;
		public PersistSQL(String ins, String upd){
			this.insertSQL=ins;
			this.updateSQL=upd;
		}
	}
	
	private Map<Class<?>, PersistSQL> registeredSQLs = new HashMap<Class<?>, PersistQuery.PersistSQL>();
	
	public PersistQuery(JdbcTemplate jdbcTemplate, 
						ISerializerRegistry serializers, 
						EntityMetadataService metadataService, 
						Class<E> entityClass){
		this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
		this.tableName = metadataService.getMetadataFor(entityClass).getEntityName();
		this.osm = new OSM(serializers);
	}
	
	public void register(Class<?> type, String insertSQL, String updateSQL){
		registeredSQLs.put(type, new PersistSQL(insertSQL, updateSQL));
	}
	
	public void persist(E rootEntity){
		WriteBatch batch = osm.writeEntity(rootEntity);
		
		WritePacket writePacket;
		while ((writePacket= batch.getNext()) != null){
			if (writePacket.getIdElement().value == null)
				performInsert(writePacket);
			else
				performUpdate(writePacket);
		}
	}

	public void persist(Collection<E> entities){
		WriteBatch batch = osm.writeEntities(entities);
		
		WritePacket writePacket;
		while ((writePacket= batch.getNext()) != null){
			if (writePacket.getIdElement().value == null)
				performInsert(writePacket);
			else
				performUpdate(writePacket);
		}
	}
	
	private void performUpdate(WritePacket writePacket) {	
		Class<?> entityClass = writePacket.getEntity().getClass();
		
		String sql = registeredSQLs.get(entityClass).updateSQL;
		if (sql == null)
			sql = SQLBuilder.toUpdateSQL(tableName, writePacket);
		
		System.out.println(sql);
		
		jdbcTemplate.update(sql, paramSourceAdapter.wrap(writePacket));
	}

	private void performInsert(WritePacket writePacket) {
		Class<?> entityClass = writePacket.getEntity().getClass();
		
		String sql = registeredSQLs.get(entityClass).insertSQL;
		if (sql == null)
			sql = SQLBuilder.toInsertSQL(tableName, writePacket);
		
		System.out.println(sql);
		
		GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
		jdbcTemplate.update(sql, paramSourceAdapter.wrap(writePacket), keyHolder);

		updatePacketAfterDBInsert(writePacket, keyHolder);
	}

	public void updatePacketAfterDBInsert(WritePacket writePacket, KeyHolder keyHolder){
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

}
