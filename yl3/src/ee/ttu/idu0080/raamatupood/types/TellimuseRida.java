package ee.ttu.idu0080.raamatupood.types;


public class TellimuseRida {
	private Toode toode;
	private Long kogus;
	
	public TellimuseRida(Toode toode, Long kogus) {
		this.toode = toode;
		this.kogus = kogus;
	}

	public long getKogus() {
		return this.kogus;
	}

	public Toode getToode() {
		return this.toode;
	}
}
