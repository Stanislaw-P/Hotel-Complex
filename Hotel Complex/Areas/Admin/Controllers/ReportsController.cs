using Microsoft.AspNetCore.Mvc;

namespace Hotel_Complex.Areas.Admin.Controllers
{
    [Area("Admin")]
    public class ReportsController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }
    }
}
