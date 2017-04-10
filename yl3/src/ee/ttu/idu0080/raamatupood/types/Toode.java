package ee.ttu.idu0080.raamatupood.types;

import java.math.BigDecimal;

public class Toode {
	private Integer kood;
	private String nimetus;
	private BigDecimal hind;
	
	public Toode(Integer kood, String nimetus, BigDecimal hind) {
		this.kood = kood;
		this.nimetus = nimetus;
		this.hind = hind;
	}
	public BigDecimal getHind() {
		return this.hind;
	}
}
