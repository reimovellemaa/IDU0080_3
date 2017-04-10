package ee.ttu.idu0080.raamatupood.types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Tellimus implements Serializable {
	private static final long serialVersionUID = 1L;
	private List<TellimuseRida> tellimuseRead;;

	public Tellimus() {
		tellimuseRead = new ArrayList<TellimuseRida>();
	}

	public void addTellimuseRida(TellimuseRida tellimuseRida){
		tellimuseRead.add(tellimuseRida);
	}
	public List<TellimuseRida> getTellimuseRead(){
		return tellimuseRead;
	}
}