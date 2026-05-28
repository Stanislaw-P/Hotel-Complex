namespace HotelComplex.Db.Models
{
    public class Booking
    {
        public uint Id { get; set; }
        public uint GuestId { get; set; }
        public ushort RoomId { get; set; }
        public uint? PartnerId { get; set; }
        public uint? ContractId { get; set; }
        public DateTime CheckInDate { get; set; }
        public DateTime CheckOutDate { get; set; }
        public string Status { get; set; } = "confirmed";
        public decimal Prepayment { get; set; } = 0;

        // Навигационные свойства
        public GuestProfile? Guest { get; set; }
        public Room? Room { get; set; }
        public CorporatePartner? Partner { get; set; }
        public Contract? Contract { get; set; }

        // Вычисляемые свойства
        public int NightsCount => (int)(CheckOutDate - CheckInDate).TotalDays;
        public decimal TotalPrice => (Room?.BasePrice ?? 0) * NightsCount;
        public decimal RemainingAmount => TotalPrice - Prepayment;

        public string StatusDisplay => Status switch
        {
            "confirmed" => "Подтверждено",
            "cancelled" => "Отменено",
            "completed" => "Завершено",
            _ => "Неизвестно"
        };

        public string StatusColor => Status switch
        {
            "confirmed" => "success",
            "cancelled" => "danger",
            "completed" => "secondary",
            _ => "secondary"
        };
    }
}
