using System.ComponentModel.DataAnnotations;

namespace Hotel_Complex.Models
{
    public class PublicBookingViewModel
    {
        // Данные для бронирования
        [Required(ErrorMessage = "Укажите дату заезда")]
        [DataType(DataType.Date)]
        [Display(Name = "Дата заезда")]
        public DateTime CheckInDate { get; set; } = DateTime.Today;

        [Required(ErrorMessage = "Укажите дату выезда")]
        [DataType(DataType.Date)]
        [Display(Name = "Дата выезда")]
        [DateGreaterThan("CheckInDate", ErrorMessage = "Дата выезда должна быть позже даты заезда")]
        public DateTime CheckOutDate { get; set; } = DateTime.Today.AddDays(1);

        [Required(ErrorMessage = "Выберите номер")]
        [Display(Name = "Номер")]
        public ushort RoomId { get; set; }

        // Личные данные гостя
        [Required(ErrorMessage = "Введите фамилию")]
        [StringLength(50, MinimumLength = 2, ErrorMessage = "Фамилия должна содержать от 2 до 50 символов")]
        [Display(Name = "Фамилия")]
        public string LastName { get; set; } = string.Empty;

        [Required(ErrorMessage = "Введите имя")]
        [StringLength(50, MinimumLength = 2, ErrorMessage = "Имя должно содержать от 2 до 50 символов")]
        [Display(Name = "Имя")]
        public string FirstName { get; set; } = string.Empty;

        [StringLength(50)]
        [Display(Name = "Отчество")]
        public string? MiddleName { get; set; }

        [Required(ErrorMessage = "Введите серию паспорта")]
        [StringLength(10, MinimumLength = 4)]
        [Display(Name = "Серия паспорта")]
        public string PassportSeries { get; set; } = string.Empty;

        [Required(ErrorMessage = "Введите номер паспорта")]
        [StringLength(20, MinimumLength = 6)]
        [Display(Name = "Номер паспорта")]
        public string PassportNumber { get; set; } = string.Empty;

        [Required(ErrorMessage = "Введите номер телефона")]
        [Phone(ErrorMessage = "Некорректный номер телефона")]
        [Display(Name = "Телефон")]
        public string Phone { get; set; } = string.Empty;

        [EmailAddress(ErrorMessage = "Некорректный email")]
        [Display(Name = "Email")]
        public string? Email { get; set; }

        // Дополнительная информация для отображения
        public int NightsCount { get; set; }
        public decimal TotalPrice { get; set; }
        public decimal PricePerNight { get; set; }
        public string RoomNumber { get; set; } = string.Empty;
        public string RoomTypeName { get; set; } = string.Empty;
        public byte Capacity { get; set; }
    }

    public class DateGreaterThanAttribute : ValidationAttribute
    {
        private readonly string _comparisonProperty;

        public DateGreaterThanAttribute(string comparisonProperty)
        {
            _comparisonProperty = comparisonProperty;
        }

        protected override ValidationResult? IsValid(object? value, ValidationContext validationContext)
        {
            var currentValue = (DateTime?)value;
            var property = validationContext.ObjectType.GetProperty(_comparisonProperty);
            var comparisonValue = (DateTime?)property?.GetValue(validationContext.ObjectInstance);

            if (currentValue.HasValue && comparisonValue.HasValue && currentValue <= comparisonValue)
            {
                return new ValidationResult(ErrorMessage);
            }

            return ValidationResult.Success;
        }
    }
}
