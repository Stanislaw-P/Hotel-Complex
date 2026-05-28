namespace HotelComplex.Db.Models
{
    public class GuestProfile
    {
        public uint UserId { get; set; }
        public string LastName { get; set; } = string.Empty;
        public string FirstName { get; set; } = string.Empty;
        public string? MiddleName { get; set; }
        public string PassportSeries { get; set; } = string.Empty;
        public string PassportNumber { get; set; } = string.Empty;
        public string Citizenship { get; set; } = "РФ";

        public string FullName => $"{LastName} {FirstName} {MiddleName}".Trim();
        public string FullPassport => $"{PassportSeries} {PassportNumber}";
    }
}
