public class JobDetails {
    String Title;
    String Company;
    String Location;
    String Type;
    String Level;
    String EXP;
    String Country;
    String Skills;
    public JobDetails(String Title, String Company, String Location, String Type, String Level, String EXP, String Country, String Skills) {
        this.Title = Title;
        this.Company = Company;
        this.Location = Location;
        this.Type = Type;
        this.Level = Level;
        this.EXP = EXP;
        this.Country = Country;
        this.Skills = Skills;

    }
    public String getTitle() {
        return Title;
    }
    public void setTitle(String Title) {
        this.Title = Title;
    }

    public String getCompany() {
        return Company;
    }
    public void setCompany(String Company) {
        this.Company = Company;
    }

    public String getLocation() {
        return Location;
    }
    public void setLocation(String Location) {
        this.Location = Location;
    }

    public String getType() {
        return Type;
    }
    public void setType(String Type) {
        this.Type = Type;
    }

    public String getLevel() {
        return Level;
    }
    public void setLevel(String Level) {
        this.Level = Level;
    }

    public String getEXP() {
        return EXP;
    }
    public void setEXP(String EXP) {
        this.EXP = EXP;
    }

    public String getCountry() {
        return Country;
    }
    public void setCountry(String Country) {
        this.Country = Country;
    }

    public String getSkills() {
        return Skills;
    }
    public void setSkills(String Skills) {
        this.Skills = Skills;
    }




}
