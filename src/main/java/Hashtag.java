import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@XmlRootElement
@Table(name="HashtagTimeseries")
@Entity
public class Hashtag implements HibernateEntity{

	private static final long serialVersionUID = 7438053049116133221L;

	@Id
	@GeneratedValue
	private int id;

	@Column
	private String tag;

	@Column
	private Date time;

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public Date getTime(){ return time; }

	public void setTime(Date time){ this.time = time; }
	
}
