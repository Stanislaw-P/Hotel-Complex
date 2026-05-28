namespace HotelComplex.Db.Models
{
    public class Contract
    {
        public uint Id { get; set; }
        public uint PartnerId { get; set; }
        public DateTime ConclusionDate { get; set; }
        public DateTime ValidUntil { get; set; }
        public decimal DiscountRate { get; set; } = 0;

        public CorporatePartner? Partner { get; set; }
    }
}
