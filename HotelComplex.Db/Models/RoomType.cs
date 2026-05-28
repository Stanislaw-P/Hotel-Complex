namespace HotelComplex.Db.Models
{
    public class RoomType
    {
        public ushort Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Description { get; set; }
    }
}
