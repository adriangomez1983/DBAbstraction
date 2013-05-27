/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package example;

import java.util.Date;

/**
 *
 * @author adrian
 */
public class Customer {
    private String phoneNumber;
    public String street;
    public String name;
    
    public Customer(String phoneNumb, String strt, String n)
    {
        phoneNumber = phoneNumb;
        street = strt;
        name = n;
    }
    
    public String getPhoneNumber()
    {
        return phoneNumber;
    }
    
    @Override
    public String toString()
    {
        return String.format("SAClient - Description: PHONE_NUMBER: %s, \nSTREET: %s, \nNAME: %s", phoneNumber, street, name);
    }
}
