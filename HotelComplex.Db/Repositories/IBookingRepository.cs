using HotelComplex.Db.Models;

namespace HotelComplex.Db.Repositories
{
    public interface IBookingRepository
    {
        // Основные CRUD
        Task<Booking?> GetByIdAsync(uint id);
        Task<IEnumerable<Booking>> GetAllAsync();
        Task<IEnumerable<Booking>> GetAllWithDetailsAsync();
        Task<Booking> CreateAsync(Booking booking);
        Task<Booking> UpdateAsync(Booking booking);
        Task<bool> DeleteAsync(uint id);

        // Управление статусом
        Task<bool> ConfirmBookingAsync(uint id);
        Task<bool> CancelBookingAsync(uint id);
        Task<bool> CompleteBookingAsync(uint id);

        // Поиск и фильтрация
        Task<IEnumerable<Booking>> GetBookingsByGuestAsync(uint guestId);
        Task<IEnumerable<Booking>> GetBookingsByRoomAsync(ushort roomId);
        Task<IEnumerable<Booking>> GetBookingsByStatusAsync(string status);
        Task<IEnumerable<Booking>> GetBookingsByDateRangeAsync(DateTime startDate, DateTime endDate);
        Task<IEnumerable<Booking>> GetActiveBookingsAsync();
        Task<IEnumerable<Booking>> GetUpcomingBookingsAsync(int days = 7);

        // Проверки
        Task<bool> IsRoomAvailableAsync(ushort roomId, DateTime checkIn, DateTime checkOut, uint? excludeId = null);
        Task<bool> HasActiveBookingsAsync(uint guestId);

        // Статистика
        Task<int> GetCountAsync();
        Task<int> GetCountByStatusAsync(string status);
        Task<decimal> GetTotalRevenueAsync(DateTime? startDate = null, DateTime? endDate = null);
        Task<Dictionary<string, int>> GetStatusStatisticsAsync();

        // Расчеты
        Task<decimal> CalculateTotalPriceAsync(ushort roomId, DateTime checkIn, DateTime checkOut);
    }
}
