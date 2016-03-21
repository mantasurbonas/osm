package gugit.osm.test.model;

import gugit.om.annotations.Column;
import gugit.om.annotations.Entity;
import gugit.om.annotations.ID;
import gugit.om.annotations.DetailEntities;
import gugit.om.annotations.DetailEntity;

import java.util.LinkedList;
import java.util.List;

@Entity(name="PERSON")
public class Person {

	@ID
	public Integer id;
	
	@Column(name="NAME")
	public String name;
	
	@DetailEntity
	public Address currentAddress;
	
	@DetailEntities(detailClass=Address.class)
	public List<Address> previousAddresses = new LinkedList<Address>();

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Address getCurrentAddress() {
		return currentAddress;
	}

	public void setCurrentAddress(Address currentAddress) {
		this.currentAddress = currentAddress;
	}

	public List<Address> getPreviousAddresses() {
		return previousAddresses;
	}

	public void setPreviousAddresses(List<Address> previousAddresses) {
		this.previousAddresses = previousAddresses;
	}

	public String toString(){
		return "Person #"+id
				+": "+name
				+" who lives at "+currentAddress+" "
				+(previousAddresses.isEmpty()?"": (" (previously, "+previousAddresses+")") );
	}
}


