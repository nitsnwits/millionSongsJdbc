package resources;

import java.util.UUID;

public class GenerateUUID {
  public static UUID uuidGenerator;
  
  public static UUID get() {
	  return uuidGenerator.randomUUID();
  }
  
}