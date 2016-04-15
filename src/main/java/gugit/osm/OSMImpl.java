package gugit.osm;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.springframework.stereotype.Service;

import gugit.om.OM;
import gugit.om.mapping.ISerializer;
import gugit.om.mapping.ReadContext;
import gugit.osm.utils.ResultsetRowIterator;


/***
 * extends gugit:: object mapper with abilities to map to SQL resultset
 * 
 * thread safe
 * 
 * @author urbonman
 */
@Service("osm")
public class OSMImpl extends OM implements OSM{
	
	public <E> List<E> readEntities(ResultSet rs, Class<E> entityClass) {		
		ISerializer<E> serializer = (ISerializer<E>)entityService.getSerializerFor(entityClass);
		ReadContext readContext = new ReadContext(entityService, entityService);
		
		ResultsetRowIterator row = new ResultsetRowIterator(rs);
		LinkedList<E> result = new LinkedList<E>();
		E previousEntity = null;
		try {
			do{			
				E entity = serializer.read(row, 0, readContext);
				if (entity != previousEntity){				
					result.add(entity);
					previousEntity = entity;				
				}
			}while (rs.next());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} 		
		return result;
	}
	
	public <E> void leftJoin(Collection<E> entities, final String property, ResultSet rs){
		if (entities.isEmpty())
			return;
		
		Iterator<E> it = entities.iterator();
		E entity = it.next();
		
		@SuppressWarnings("unchecked")
		ISerializer<E> serializer = (ISerializer<E>)entityService.getSerializerFor(entity.getClass());
		int propIndex = serializer.getPropertyIndex(property);
		
		ReadContext readContext = new ReadContext(entityService, entityService);
		ResultsetRowIterator row = new ResultsetRowIterator(rs);
		
		try {
			do{			
				Object id = row.peek(0);
				while(!id.equals(serializer.getID(entity))){
					if (it.hasNext())
						entity=it.next();
					else
						return;
				}
				
				serializer.leftJoin(entity, propIndex, row, 0, readContext);
			}while (rs.next());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} 
	}
	
	public <E> void registerType(Class<E> entityClass){
		entityService.getMetadataFor(entityClass);
	}
}
