using System.ComponentModel.DataAnnotations;

namespace Hotel_Complex.Models
{
    public class ServiceOrderViewModel
    {
        public int Id { get; set; }

        [Required(ErrorMessage = "Выберите услугу")]
        [Display(Name = "Услуга")]
        public int ServiceId { get; set; }

        [Display(Name = "Название услуги")]
        public string ServiceName { get; set; } = string.Empty;

        [Required(ErrorMessage = "Укажите дату")]
        [DataType(DataType.Date)]
        [Display(Name = "Дата заказа")]
        public DateTime OrderDate { get; set; } = DateTime.Today;

        [Display(Name = "Цена")]
        public decimal Price { get; set; }

        [Display(Name = "Количество")]
        [Range(1, 10, ErrorMessage = "Количество от 1 до 10")]
        public int Quantity { get; set; } = 1;

        [Display(Name = "Комментарий к заказу")]
        [DataType(DataType.MultilineText)]
        [StringLength(500, ErrorMessage = "Максимум 500 символов")]
        public string? Comment { get; set; }

        [Display(Name = "Номер проживания")]
        public int StayId { get; set; }

        [Display(Name = "Общая стоимость")]
        public decimal TotalPrice => Price * Quantity;

        [Display(Name = "Номер комнаты")]
        public string RoomNumber { get; set; } = string.Empty;

        [Display(Name = "Гость")]
        public string GuestName { get; set; } = string.Empty;
    }
}
