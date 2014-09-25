/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import java.util.Random;

/**
 *
 * @author anantoni
 */
public class RandomGenerator {
     private static final Random generator = new Random(System.currentTimeMillis());
     
     public static Random getRandomGenerator() {
         return generator;
     }
}
