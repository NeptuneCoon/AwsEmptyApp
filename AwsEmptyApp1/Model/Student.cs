using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsEmptyApp1.Model
{
    public class Student
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class Test
    {
        public void Testing(Student stu1,Student stu2,object obj = null)
        {
            // to do nothing...
        }
    }

    public class Client
    {
        public void UseTestingMethod()
        {

        }
    }
}
