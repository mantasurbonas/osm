package gugit.osm.utils;

import gugit.om.mapping.EntityWritePacket;
import gugit.om.mapping.M2MWritePacket;
import gugit.om.mapping.NullWriteValue;
import gugit.om.mapping.WritePacketElement;
import gugit.om.utils.StringTemplate;

public class SQLBuilder {

	
	private static final String DELETE_FROM_M2M_SQL_TEMPLATE = 
		"DELETE FROM %M2M_TABLE_NAME% "
		+  "WHERE %A_COLUMN%=:%A_ID_VALUE% "
		+  "AND %B_COLUMN% NOT IN (:%B_ID_VALUES%)";

	private static final String INSERT_INTO_M2M_SQL_TEMPLATE = 
		"INSERT INTO %M2M_TABLE_NAME%(%A_COLUMN%, %B_COLUMN%) \n"
		+  "SELECT :%A_ID_VALUE%, __tbl1.%B_ID_COLUMN% \n"
		+  "FROM %TABLE_B_NAME% __tbl1 \n"
		+  "WHERE __tbl1.%B_ID_COLUMN% IN (:%B_ID_VALUES%) \n"
		+  "AND __tbl1.%B_ID_COLUMN% NOT IN \n"
		+  "(  SELECT %B_COLUMN% \n"
		+  "   FROM %M2M_TABLE_NAME% \n"
		+  "   WHERE %A_COLUMN% = :%A_ID_VALUE% )";

	
	public static String columnNameToParamName(String columnName) {
		return columnName
					.replaceAll("\"", "")
					.replaceAll("'", "")
					.replaceAll("-", "_")
					.replaceAll(".", "_")
					.replaceAll(" ", "")
					.replaceAll("\t", "")
					.replaceAll("\n", "")
					.replaceAll("\r", "")
					.trim()
					.toLowerCase();
	}
	
	public static String[] toSQL(M2MWritePacket writePacket){
		return new String[]{toRemoveObsoleteBindingsSQL(writePacket),
							toInsertBindingsSQL(writePacket)};
	}

	private static String toRemoveObsoleteBindingsSQL(M2MWritePacket writePacket) {
		return new StringTemplate(DELETE_FROM_M2M_SQL_TEMPLATE)
					.replace("M2M_TABLE_NAME", writePacket.getEntityName())
					.replace("A_COLUMN", writePacket.leftSideCol)
					.replace("A_ID_VALUE", writePacket.leftSideField)
					.replace("B_COLUMN", writePacket.rightSideCol)
					.replace("B_ID_VALUES", writePacket.rightSideField)
					.getResult();
	}
	
	public static String toInsertBindingsSQL(M2MWritePacket writePacket) {	
		return new StringTemplate(INSERT_INTO_M2M_SQL_TEMPLATE)
					.replace("M2M_TABLE_NAME", writePacket.getEntityName())
					.replace("A_COLUMN", writePacket.leftSideCol)
					.replace("A_ID_VALUE", writePacket.leftSideField)
					.replace("B_COLUMN", writePacket.rightSideCol)
					.replace("B_ID_VALUES", writePacket.rightSideField)
					.replace("TABLE_B_NAME", writePacket.rightSideTable)
					.replace("B_ID_COLUMN", writePacket.rightSideTableId)
					.getResult();
	}

	public static String toSQL(EntityWritePacket writePacket) {
		if (writePacket.getIdElement().value == null)
			return toInsertSQL(writePacket);
		
		return toUpdateSQL(writePacket);
	}
	
	public static String toInsertSQL(EntityWritePacket writePacket) {
		StringBuilder cols = new StringBuilder();
		StringBuilder vals   = new StringBuilder();
		String joinStr = "";
		
		for (WritePacketElement data: writePacket.getElements()){
			if (data.value==NullWriteValue.getInstance())
				continue;
			
			cols.append(joinStr);
			cols.append(data.columnName);

			vals.append(joinStr);
			vals.append(":");
			vals.append(data.fieldName);
			
			joinStr = ", ";
		}
		
		return "INSERT INTO "
				+writePacket.getEntityName()
				+" ("
				+ cols
				+") VALUES ("
				+ vals
				+")";
	}

	public static String toUpdateSQL(EntityWritePacket writePacket) {
		
		if (writePacket.getIdElement().value == NullWriteValue.getInstance())
			throw new RuntimeException("ID value not specified!");
		
		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ")
			.append(writePacket.getEntityName())
			.append(" SET ");
		
		String joinStr = "";
		
		for (WritePacketElement data: writePacket.getElements()){
			sql.append(joinStr);
			sql.append(data.columnName);
			sql.append("=");
			if (data.value==NullWriteValue.getInstance())
				sql.append("null");
			else
				sql.append(":").append(data.fieldName);
			joinStr = ", ";
		}
		
		sql.append(" WHERE ")
			.append(writePacket.getIdElement().columnName)
			.append("=:")
			.append(writePacket.getIdElement().fieldName);
		
		return sql.toString();
	}

	
}
