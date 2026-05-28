namespace HotelComplex.Db.Models
{
    public class CorporatePartner
    {
        public uint Id { get; set; }
        public string OrganizationName { get; set; } = string.Empty;
        public string LegalAddress { get; set; } = string.Empty;
        public string ContactPerson { get; set; } = string.Empty;
        public string Phone { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
    }
}
