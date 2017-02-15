package gugit.osm;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;

import gugit.om.mapping.WriteBatch;
import gugit.om.utils.IDataIterator;


/***
 * interface to a (stateless) Object-SQL Mapping service
 * 
 * @author urbonman
 *
 */
public interface OSM {

	<E> void registerType(Class<E> entityClass);
	
	<E> WriteBatch writeEntity(E entity);
	
	<E> void writeEntity(E entity, WriteBatch batch);
	
	<E> WriteBatch writeEntities(Collection<E> entities);
	
	<E> void writeEntities(Collection<E> entities, WriteBatch writeBatch);
	
	<E> E readEntity(IDataIterator<Object> row, Class<E> entityClass);
	
	<E> List<E> readEntities(ResultSet resultset, Class<E> entityClass); 
	
	<E> E leftJoin(E entity, final String property, IDataIterator<Object> row);
	
	<E> void leftJoin(Collection<E> entities, final String property, ResultSet resultset);
	
}
