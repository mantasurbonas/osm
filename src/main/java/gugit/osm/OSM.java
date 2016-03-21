package gugit.osm;

import gugit.om.OM;
import gugit.om.mapping.ISerializer;
import gugit.om.mapping.ISerializerRegistry;
import gugit.osm.utils.ResultsetRowIterator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;


/***
 * extends gugit:: object mapper with abilities to map to SQL resultset
 * @author urbonman
 */
public class OSM extends OM{
	
	public OSM(ISerializerRegistry serializers) {
		super(serializers);
	}
	
	public <E> List<E> readEntities(ResultSet rs, Class<E> entityClass) {
		reset();
		
		LinkedList<E> result = new LinkedList<E>();

		ISerializer<E> serializer = (ISerializer<E>)serializers.getSerializerFor(entityClass);
		
		E previousEntity = null;
		ResultsetRowIterator rowIterator = new ResultsetRowIterator(rs);
		try {
			do{			
				E entity = serializer.read(rowIterator, 0, readContext);
				if (entity == previousEntity)
					continue;
				
				result.add(entity);
				previousEntity = entity;
			}while (rs.next());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} 		
		return result;
	}
		
}
