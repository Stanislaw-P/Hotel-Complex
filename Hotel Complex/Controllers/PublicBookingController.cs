using Hotel_Complex.Models;
using Microsoft.AspNetCore.Mvc;

namespace Hotel_Complex.Controllers
{
    public class PublicBookingController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Book()
        {
            return View();
        }

        /// <summary>
        /// GET: /PublicBooking/Book
        /// Страница бронирования конкретного номера
        /// </summary>
        /// <param name="roomId">ID номера (101, 201, 301 и т.д.)</param>
        /// <param name="checkIn">Дата заезда (yyyy-MM-dd)</param>
        /// <param name="checkOut">Дата выезда (yyyy-MM-dd)</param>
        //[HttpGet]
        //public IActionResult Book(ushort roomId, DateTime? checkIn, DateTime? checkOut)
        //{
        //    // Статические данные номеров (временное решение)
        //    var rooms = GetRoomsStaticData();

        //    // Проверяем существование номера
        //    if (!rooms.ContainsKey(roomId))
        //    {
        //        return NotFound("Номер не найден");
        //    }

        //    var selectedRoom = rooms[roomId];

        //    // Устанавливаем даты
        //    var today = DateTime.Today;
        //    var checkInDate = checkIn ?? today;
        //    var checkOutDate = checkOut ?? today.AddDays(1);

        //    // Валидация дат
        //    if (checkOutDate <= checkInDate)
        //    {
        //        checkOutDate = checkInDate.AddDays(1);
        //    }

        //    if (checkInDate < today)
        //    {
        //        checkInDate = today;
        //        checkOutDate = today.AddDays(1);
        //    }

        //    var nights = (int)(checkOutDate - checkInDate).TotalDays;
        //    var totalPrice = selectedRoom.BasePrice * nights;

        //    // Создаем ViewModel и передаем в представление
        //    var model = new PublicBookingViewModel
        //    {
        //        RoomId = roomId,
        //        CheckInDate = checkInDate,
        //        CheckOutDate = checkOutDate,
        //        RoomNumber = selectedRoom.RoomNumber.ToString(),
        //        RoomTypeName = selectedRoom.RoomTypeName,
        //        Capacity = selectedRoom.Capacity,
        //        PricePerNight = selectedRoom.BasePrice,
        //        NightsCount = nights,
        //        TotalPrice = totalPrice
        //    };

        //    return View(model);
        //}

        ///// <summary>
        ///// POST: /PublicBooking/Book
        ///// Обработка отправленной формы бронирования
        ///// </summary>
        //[HttpPost]
        //[ValidateAntiForgeryToken]
        //public async Task<IActionResult> Book(PublicBookingViewModel model)
        //{
        //    // Статические данные номеров
        //    var rooms = GetRoomsStaticData();

        //    // Проверяем существование номера
        //    if (!rooms.ContainsKey(model.RoomId))
        //    {
        //        ModelState.AddModelError("RoomId", "Выбранный номер не существует");
        //    }

        //    // Проверяем доступность номера на выбранные даты
        //    var isAvailable = IsRoomAvailable(model.RoomId, model.CheckInDate, model.CheckOutDate);
        //    if (!isAvailable)
        //    {
        //        ModelState.AddModelError("", "Извините, этот номер уже забронирован на выбранные даты");
        //    }

        //    // Валидация дат
        //    if (model.CheckInDate < DateTime.Today)
        //    {
        //        ModelState.AddModelError("CheckInDate", "Дата заезда не может быть в прошлом");
        //    }

        //    if (model.CheckOutDate <= model.CheckInDate)
        //    {
        //        ModelState.AddModelError("CheckOutDate", "Дата выезда должна быть позже даты заезда");
        //    }

        //    if (ModelState.IsValid)
        //    {
        //        // В реальном приложении здесь нужно:
        //        // 1. Создать гостя (если его нет)
        //        // 2. Создать бронирование
        //        // 3. Сохранить в БД
        //        // 4. Отправить email/смс уведомление

        //        _logger.LogInformation($"Новое бронирование: Номер {model.RoomNumber}, " +
        //            $"Гость: {model.LastName} {model.FirstName}, " +
        //            $"Даты: {model.CheckInDate:yyyy-MM-dd} - {model.CheckOutDate:yyyy-MM-dd}");

        //        // Сохраняем данные в TempData для отображения на странице успеха
        //        TempData["BookingSuccess"] = true;
        //        TempData["BookingId"] = new Random().Next(1000, 9999); // Временный ID
        //        TempData["RoomNumber"] = model.RoomNumber;
        //        TempData["RoomTypeName"] = model.RoomTypeName;
        //        TempData["CheckInDate"] = model.CheckInDate.ToString("dd.MM.yyyy");
        //        TempData["CheckOutDate"] = model.CheckOutDate.ToString("dd.MM.yyyy");
        //        TempData["NightsCount"] = model.NightsCount;
        //        TempData["TotalPrice"] = model.TotalPrice.ToString("N0");
        //        TempData["GuestName"] = $"{model.LastName} {model.FirstName} {model.MiddleName}".Trim();

        //        // Перенаправляем на страницу успеха
        //        return RedirectToAction("BookingSuccess");
        //    }

        //    // Если есть ошибки, возвращаем форму с заполненными данными
        //    var selectedRoom = rooms[model.RoomId];
        //    model.RoomNumber = selectedRoom.RoomNumber.ToString();
        //    model.RoomTypeName = selectedRoom.RoomTypeName;
        //    model.Capacity = selectedRoom.Capacity;
        //    model.PricePerNight = selectedRoom.BasePrice;
        //    model.NightsCount = (int)(model.CheckOutDate - model.CheckInDate).TotalDays;
        //    model.TotalPrice = selectedRoom.BasePrice * model.NightsCount;

        //    return View(model);
        //}
    }
}
