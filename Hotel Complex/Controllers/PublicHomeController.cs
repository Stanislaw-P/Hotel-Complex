using Microsoft.AspNetCore.Mvc;

namespace Hotel_Complex.Controllers
{
    public class PublicHomeController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }
    }
}
