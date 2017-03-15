package gugit.osm.jdbctemplate;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import gugit.om.mapping.EntityWritePacket;
import gugit.om.mapping.IWritePacket;
import gugit.om.mapping.M2MWritePacket;
import gugit.om.mapping.WriteBatch;
import gugit.om.mapping.WritePacketElement;
import gugit.om.wrapping.EntityMarkingHelper;
import gugit.osm.OSM;

import javax.sql.DataSource;

/***
 * a convenient wrapper around JDBC NamedTemplate and gugit OSM (for writing entities to SQL)
 * 
 * @author urbonman
 */
@Service
@Scope("prototype")
public class PersistanceService{

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	@Autowired
	private OSM osm;
	
	private SqlStatementRegistryImpl sqls = new SqlStatementRegistryImpl();
	
	private static final Logger logger = LogManager.getLogger();

	public <E> void persist(E rootEntity){
		persistWriteBatch(osm.writeEntity(rootEntity));
	}

	public <E> void persist(Collection<E> entities){
		persistWriteBatch(osm.writeEntities(entities));
	}
	
	public <T> SelectQuery<T> query(final String sql, Class<T> entityClass){
		return new SelectQuery<T>(namedParameterJdbcTemplate, osm, entityClass).setSql(sql);
	}
	
	public <T> JoinQuery<T> leftJoin(Collection<T> entities, String propertyName, String sql){
		return new JoinQuery<T>(namedParameterJdbcTemplate, osm, propertyName)
							.setEntities(entities)
							.setSql(sql);
	}
	
	private void persistWriteBatch(WriteBatch batch) {
		SqlParameterSourceAdapter paramSourceAdapter = new SqlParameterSourceAdapter();
		
		IWritePacket writePacket;
		while ((writePacket= batch.getNext()) != null){
			if (writePacket instanceof EntityWritePacket)
				write((EntityWritePacket)writePacket,paramSourceAdapter);
			else
				write((M2MWritePacket)writePacket,paramSourceAdapter);
		}
	}

	private void write(M2MWritePacket writePacket, SqlParameterSourceAdapter paramSourceAdapter) {
		String[] sqls = getM2MBindingSql(writePacket);
		if (sqls == null){
			logger.warn("skipping update of "+writePacket.getEntityName());
			return;
		}
		
		logger.debug("persisting many-to-many relationship");
		logger.debug(sqls[0]);
		logger.debug(sqls[1]);
		
		SqlParameterSource paramSource = paramSourceAdapter.wrap(writePacket);
		
		namedParameterJdbcTemplate.update(sqls[0], paramSource);
		namedParameterJdbcTemplate.update(sqls[1], paramSource);
	}

	private void write(EntityWritePacket writePacket, SqlParameterSourceAdapter paramSourceAdapter){
		if (writePacket.getIdElement().value == null)
			performInsert(writePacket, paramSourceAdapter.wrap(writePacket));
		else
			performUpdate(writePacket, paramSourceAdapter.wrap(writePacket));			
	}
	
	private void performUpdate(EntityWritePacket writePacket, SqlParameterSource paramSource) {
		String updateSql = getUpdateSql(writePacket);
		
		if (updateSql == null){
			System.out.println("skipping update of "+writePacket.getEntityName());
			return;
		}
		
		logger.debug(updateSql);
		
		namedParameterJdbcTemplate.update(updateSql, paramSource);
	}

	private void performInsert(EntityWritePacket writePacket, SqlParameterSource paramSource) {				
		String insertSql = getInsertSql(writePacket);
		
		if (insertSql == null){
			logger.warn("skipping insert of "+writePacket.getEntityName());
			return;
		}
		
		logger.debug(insertSql);
		
		GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
		namedParameterJdbcTemplate.update(insertSql, paramSource, keyHolder);
		updatePacketAfterDBInsert(writePacket, keyHolder);
	}
	
	private static void updatePacketAfterDBInsert(EntityWritePacket writePacket, KeyHolder keyHolder){
		WritePacketElement idElement = writePacket.getIdElement();
		
		Object idVal = findIDValue(idElement.columnName, keyHolder.getKeyList());
		
		if (idVal == null){
			logger.warn("WARNING: "
							+ "could not find ID ('"+idElement.columnName+"') "
							+ "in the keylist "+keyHolder.getKeyList()+". "
							+ "Did the insert succeed?");
		}
		else{
			writePacket.updateIDValue(idVal);
		}
		// TODO: potentially update other properties as well
		
		// performing insert does change object's id thus automatically makes it 'dirty'.
		// explicitely stating that the (just persisted) object is NOT dirty at this moment:
		EntityMarkingHelper.setDirty(writePacket.getEntity(), false);
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

	public class RegisterEntityHelper{
		private Class<?> type;
		
		RegisterEntityHelper(Class<?> type){
			this.type = type;
		} 
		
		public RegisterEntityHelper update(final String updateSql){
			sqls.registerUpdateSql(type, updateSql);
			return this;
		}
		
		public RegisterEntityHelper insert(final String insertSql){
			sqls.registerInsertSql(type, insertSql);
			return this;
		}
		
		public RegisterEntityHelper noUpdates(){
			sqls.registerNoUpdateSql(type);
			return this;
		}
		
		public RegisterEntityHelper noInserts(){
			sqls.registerNoInsertSql(type);
			return this;
		}
		
		public RegisterEntityHelper readonly(){
			sqls.registerNoInsertSql(type);
			sqls.registerNoUpdateSql(type);
			return this;
		}
	}
	
	public class RegisterM2MHelper{
		String tablename;
		public RegisterM2MHelper(String tablename){
			this.tablename = tablename;
		}
		
		public RegisterM2MHelper noWrites(){
			sqls.registerNoM2MWrites(tablename);
			return this;
		}
		
		public RegisterM2MHelper write(String[] writeSqls){
			sqls.registerWriteSqls(tablename, writeSqls);
			return this;
		}
	}
	
	public RegisterEntityHelper register(Class<?> type) {
		type = EntityMarkingHelper.getEntityClass(type);
		sqls.registerEntity(type);
		osm.registerType(type);
		return new RegisterEntityHelper(type);
	}

	public RegisterM2MHelper register(String tablename){
		sqls.registerM2M(tablename);
		return new RegisterM2MHelper(tablename);
	}
	
	public String getUpdateSql(EntityWritePacket writePacket) {
		return sqls.getUpdateSql(writePacket);
	}

	public String getInsertSql(EntityWritePacket writePacket) {
		return sqls.getInsertSql(writePacket);
	}
	
	public String[] getM2MBindingSql(M2MWritePacket writePacket){
		return sqls.getWriteSqls(writePacket);
	}

	public PersistanceService setDataSource(DataSource dataSource) {
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
		return this;
	}

}
