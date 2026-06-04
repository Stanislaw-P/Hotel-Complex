namespace Hotel_Complex.Models
{
    public class RegistrationResult
    {
        public bool Success { get; set; }
        public List<string> Errors { get; set; } = new List<string>();

        public static RegistrationResult Ok() => new RegistrationResult { Success = true };

        public static RegistrationResult Fail(string error) => new RegistrationResult
        {
            Success = false,
            Errors = { error }
        };

        public static RegistrationResult Fail(IEnumerable<string> errors) => new RegistrationResult
        {
            Success = false,
            Errors = errors.ToList()
        };
    }
}
