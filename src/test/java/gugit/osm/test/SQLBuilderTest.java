package gugit.osm.test;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import gugit.om.mapping.EntityWritePacket;
import gugit.om.mapping.IPropertyAccessor;
import gugit.om.mapping.M2MWritePacket;
import gugit.osm.test.model.Address;
import gugit.osm.utils.SQLBuilder;

public class SQLBuilderTest {

	@Test
	public void testInsertSQL() {
		EntityWritePacket writePacket = new EntityWritePacket("", "PERSON");
		writePacket.addElement("NAME", "name", "labas");
		String sql = SQLBuilder.toInsertSQL(writePacket );
		
		assertEquals(sql, "INSERT INTO PERSON (NAME) VALUES (:name)");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testUpdateSQL() {
		EntityWritePacket writePacket = new EntityWritePacket("", "PERSON");
		
		writePacket.addElement("NAME", "name", "labas");
		writePacket.setID("ID", "id", new IPropertyAccessor() {
			public Object getValue(Object arg0) { return 10; }
			public void setValue(Object arg0, Object arg1){}});
		
		String sql = SQLBuilder.toUpdateSQL(writePacket );
		
		assertEquals(sql, "UPDATE PERSON SET NAME=:name WHERE ID=:id");
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testWriteSQLs(){
		M2MWritePacket writePacket = new M2MWritePacket("SPACESHIP_TO_EARTH");
		
		writePacket.setLeftSideDependency("SPACESHIP_ID", "spaceship", new String(), new IPropertyAccessor(){
															public void setValue(Object entity, Object value) {	}
															public Object getValue(Object entity) { return 11; }
		});
		List<Address> entities = new LinkedList<Address>();
		entities.add(new Address());
		writePacket.setRightSideDependency("EARTH_ID", "earth", entities, new IPropertyAccessor(){
															public void setValue(Object entity, Object value) {}
															public Object getValue(Object entity) {return 12; }},
											"EARTH", "ID");
		
		writePacket.trySolveDependencies();
		
		String[] sqls = SQLBuilder.toSQL(writePacket);
		
		assertEquals(sqls.length, 2);

		assertEquals(sqls[0], "DELETE FROM SPACESHIP_TO_EARTH WHERE SPACESHIP_ID=:spaceship AND EARTH_ID NOT IN (:earth)");
	}
}
