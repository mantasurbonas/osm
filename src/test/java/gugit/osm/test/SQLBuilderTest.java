package gugit.osm.test;

import static org.junit.Assert.assertEquals;
import gugit.om.mapping.IPropertyAccessor;
import gugit.om.mapping.WritePacket;
import gugit.osm.utils.SQLBuilder;

import org.junit.Test;

public class SQLBuilderTest {

	@Test
	public void testInsertSQL() {

		WritePacket writePacket = new WritePacket("", "PERSON");
		writePacket.addElement("NAME", "name", "labas");
		String sql = SQLBuilder.toInsertSQL(writePacket );
		
		assertEquals(sql, "INSERT INTO PERSON (NAME) VALUES (:name)");
	}

	@Test
	public void testUpdateSQL() {
		WritePacket writePacket = new WritePacket("", "PERSON");
		
		writePacket.addElement("NAME", "name", "labas");
		writePacket.setID("ID", "id", new IPropertyAccessor() {
			public Object getValue(Object arg0) { return 10; }
			public void setValue(Object arg0, Object arg1){}});
		
		String sql = SQLBuilder.toUpdateSQL(writePacket );
		
		assertEquals(sql, "UPDATE PERSON SET NAME=:name WHERE ID=:id");
	}
}
