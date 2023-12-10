package unex.model;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.util.UUID;

@Entity
public class Clientes {
    @PartitionKey
    private UUID ClientesId;
    private String nombre;
    private String correoElectronico;
    private String telefono;

    // Constructor, getters y setters

    public Clientes() {
        // Constructor vacío requerido por el mapeador de objetos
    }

    public Clientes(UUID ClientesId, String nombre, String correoElectronico, String telefono) {
        this.ClientesId = ClientesId;
        this.nombre = nombre;
        this.correoElectronico = correoElectronico;
        this.telefono = telefono;
    }

    public UUID getClientesId() {
        return ClientesId;
    }

    public void setClientesId(UUID ClientesId) {
        this.ClientesId = ClientesId;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getCorreoElectronico() {
        return correoElectronico;
    }

    public void setCorreoElectronico(String correoElectronico) {
        this.correoElectronico = correoElectronico;
    }

    public String getTelefono() {
        return telefono;
    }

    public void setTelefono(String telefono) {
        this.telefono = telefono;
    }

    @Override
    public String toString() {
        return "Clientes{" +
                "ClientesId=" + ClientesId +
                ", nombre='" + nombre + '\'' +
                ", correoElectronico='" + correoElectronico + '\'' +
                ", telefono='" + telefono + '\'' +
                '}';
    }
}
