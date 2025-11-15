from pydantic import BaseModel, Field, ConfigDict
from datetime import date
from typing import Optional


class Users(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    user_id: int = Field(description="Unique identifier for the user")
    first_name: str = Field(description="First name of the user")
    last_name: str = Field(description="Last name of the user")
    email: str = Field(description="Email address of the user")
    age: int = Field(ge=0, description="Age of the user")
    gender: str = Field(description="Gender of the user")
    post_code: str = Field(description="Postal code of the user's address")
    country: str = Field(description="Country of the user")
    join_date: date = Field(description="Date when the user joined")
    from_platform: str = Field(description="Platform from which the user joined")
    
class Products(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    item_id: int = Field(description="Unique identifier for the product")
    brand: str = Field(description="Brand of the product")
    product_name: str = Field(description="Name of the product")
    eye_size: float = Field(ge=0, description="Eye size of the product")
    lens_color: str = Field(description="Color of the lens")
    price: float = Field(ge=0, description="Price of the product")
    polarized_glasses: bool = Field(description="Indicates if the lens is polarized")
    prescribed_glasses: bool = Field(description="Indicates if the glasses are prescribed")
    is_active: str = Field(description="Indicates if the product is active or inactive")
    list_date: date = Field(description="Date when the product was listed")
    discontinued_date: Optional[date] = Field(default=None, description="Date when the product was discontinued")
    
class Orders(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    order_id: int = Field(description="Unique identifier for the order")
    user_id: int = Field(description="Identifier for the user who placed the order")
    item_id: int = Field(description="Identifier for the product ordered")
    purchase_date: date = Field(description="Date when the purchase was made")
    payment_type: str = Field(description="Type of payment used for the order")
    
class Interactions(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    interaction_id: int = Field(description="Type of interaction performed by the user")
    user_id: int = Field(description="Identifier for the user involved in the interaction")
    item_id: int = Field(description="Identifier for the product involved in the interaction")
    interaction_date: date = Field(description="Date when the interaction occurred")
    
class InteractionTypes(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int = Field(description="Unique identifier for the interaction type")
    interaction_type: str = Field(description="Description of the interaction type")