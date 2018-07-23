import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@XmlRootElement
@Table(name="MentionTimeseries")
@Entity
public class Mention implements HibernateEntity{
	
	private static final long serialVersionUID = 5439625282647428918L;

	@Id
	@GeneratedValue
	private int id;

	@Column
	private String name;

	@Column
	private Date time;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getTime(){ return time; }

	public void setTime(Date time){ this.time = time; }


}
