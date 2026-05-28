using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HotelComplex.Db.Models
{
    public class Room
    {
        public ushort Id { get; set; }
        public ushort RoomNumber { get; set; }
        public byte Floor { get; set; }
        public ushort RoomTypeId { get; set; }
        public byte Capacity { get; set; }
        public decimal BasePrice { get; set; }
        public string Status { get; set; } = "free";

        // Навигационное свойство
        public RoomType? RoomType { get; set; }

        public string StatusDisplay => Status switch
        {
            "free" => "Свободен",
            "occupied" => "Занят",
            "cleaning" => "На уборке",
            "repair" => "На ремонте",
            _ => "Неизвестно"
        };

        public string StatusColor => Status switch
        {
            "free" => "success",
            "occupied" => "danger",
            "cleaning" => "warning",
            "repair" => "secondary",
            _ => "secondary"
        };
    }
}
