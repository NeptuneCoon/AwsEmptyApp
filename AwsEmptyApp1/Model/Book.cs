using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsEmptyApp1.Model
{
    public class Book
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public Address Address { get; set; }
    }

    public class Address
    {
        public string Street { get; set; }
        public Telecome Telecome { get; set; }
    }
    public class Telecome
    {
        public string Tel { get; set; }
        public string Area { get; set; }
    }
}
