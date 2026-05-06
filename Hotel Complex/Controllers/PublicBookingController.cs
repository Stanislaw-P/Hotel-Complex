using Microsoft.AspNetCore.Mvc;

namespace Hotel_Complex.Controllers
{
    public class PublicBookingController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }
    }
}
