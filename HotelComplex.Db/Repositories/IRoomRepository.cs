using HotelComplex.Db.Models;

namespace HotelComplex.Db.Repositories
{
    public interface IRoomRepository
    {
        Task<Room?> GetByIdAsync(ushort id);
        Task<Room?> GetByRoomNumberAsync(ushort roomNumber);
        Task<IEnumerable<Room>> GetAllAsync();
        Task<IEnumerable<Room>> GetAllWithTypesAsync();
        Task<Room> CreateAsync(Room room);
        Task<Room> UpdateAsync(Room room);
        Task<bool> DeleteAsync(ushort id);

        // Фильтрация
        Task<IEnumerable<Room>> GetAvailableRoomsAsync();
        Task<IEnumerable<Room>> GetRoomsByStatusAsync(string status);
        Task<IEnumerable<Room>> GetRoomsByFloorAsync(byte floor);
        Task<IEnumerable<Room>> GetRoomsByTypeAsync(ushort roomTypeId);

        // Проверки
        Task<bool> RoomNumberExistsAsync(ushort roomNumber, ushort? excludeId = null);
        Task<bool> IsRoomAvailableAsync(ushort roomId, DateTime checkIn, DateTime checkOut);

        // Статус
        Task<bool> ChangeStatusAsync(ushort roomId, string status);

        // Статистика
        Task<int> GetCountAsync();
        Task<int> GetAvailableCountAsync();
        Task<Dictionary<string, int>> GetStatusStatisticsAsync();
    }
}
