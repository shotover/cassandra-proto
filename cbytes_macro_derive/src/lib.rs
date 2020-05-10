extern crate proc_macro;

use quote::quote;
use proc_macro::TokenStream;
use syn::{DeriveInput, Type};
use syn::DataStruct;
use syn::Data;
use std::any::Any;

#[proc_macro_derive(IntoCbytes)]
pub fn into_cbytes_derivce(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    let name = ast.ident;
    let mut fields: Vec<proc_macro2::TokenStream> = Vec::new();

    match ast.data {
        Data::Struct(dataStruct ) => {
            for field in dataStruct.fields {
                if let Some(name) = field.ident {
                    //handle Optional types
                    let gen = quote! {
                    v.extend(self.#name.into_cbytes());

                };
                    fields.push(gen);
                }
            }
        },
        _ => {}
    }


    let gen = quote! {
        impl IntoBytes for #name {
            fn into_cbytes(&self) -> Vec<u8> {
                let mut v: Vec<u8> = vec![];
                #(#fields)*
                v
            }
        }
    };
    gen.into()
}