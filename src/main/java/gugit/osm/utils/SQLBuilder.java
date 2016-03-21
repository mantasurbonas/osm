package gugit.osm.utils;

import gugit.om.mapping.NullWriteValue;
import gugit.om.mapping.WritePacket;
import gugit.om.mapping.WritePacketElement;

public class SQLBuilder {

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
	
	public static String toSQL(final String tableName, WritePacket writePad) {
		if (writePad.getIdElement().value == null)
			return toInsertSQL(tableName, writePad);
		
		return toUpdateSQL(tableName, writePad);
	}
	
	public static String toInsertSQL(final String tableName, WritePacket writePacket) {
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
				+tableName
				+" ("
				+ cols
				+") VALUES ("
				+ vals
				+")";
	}

	public static String toUpdateSQL(final String tableName, WritePacket writePad) {
		
		if (writePad.getIdElement().value == NullWriteValue.getInstance())
			throw new RuntimeException("ID value not specified!");
		
		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ")
			.append(tableName)
			.append(" SET ");
		
		String joinStr = "";
		
		for (WritePacketElement data: writePad.getElements()){
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
			.append(writePad.getIdElement().columnName)
			.append("=:")
			.append(writePad.getIdElement().fieldName);
		
		return sql.toString();
	}
	
}
