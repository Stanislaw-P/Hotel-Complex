namespace HotelComplex.Db.Models
{
    public class Role
    {
        public uint Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Description { get; set; }
    }
}
